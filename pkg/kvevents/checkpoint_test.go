// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvevents

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	zmq4 "github.com/go-zeromq/zmq4"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

const testFingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestCheckpointPathForWriterIsStableAndIsolated(t *testing.T) {
	directory := t.TempDir()
	first, err := CheckpointPathForWriter(directory, "epp-a")
	require.NoError(t, err)
	repeated, err := CheckpointPathForWriter(directory, "epp-a")
	require.NoError(t, err)
	second, err := CheckpointPathForWriter(directory, "epp-b")
	require.NoError(t, err)
	require.Equal(t, first, repeated)
	require.NotEqual(t, first, second)
	require.Equal(t, directory, filepath.Dir(first))
	_, err = CheckpointPathForWriter(directory, "")
	require.Error(t, err)
}

func TestCheckpointRoundTripIncludesAppliedSourceHeader(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.adapter = &sourceEndpointAdapter{}
	msg := &RawMessage{
		Sequence: 41, Payload: []byte{11},
		SourceEndpoint: "10.0.0.1:8000", SourceDataParallelRank: intPtr(2),
		EventSourceID: "vortex/vllm-a", EventEndpoint: "tcp://10.0.0.1:5559",
	}
	pool.processRawMessage(t.Context(), msg)
	pool.groupCatalog.Learn("10.0.0.1:8000", 3, kvblock.GroupMetadata{
		Kind: string(KVCacheSpecKindMlaAttention), BlockSize: 16,
	})
	pool.dedup.trackStore(blockScope{
		podIdentifier: "10.0.0.1:8000", deviceTier: "gpu",
		groupIdx: noGroupIdx, dataParallelRank: 2,
	}, []uint64{11, 11})
	path := filepath.Join(t.TempDir(), "index.checkpoint")

	result, err := pool.WriteCheckpoint(path, testFingerprint)
	require.NoError(t, err)
	require.Equal(t, 1, result.Sources)

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	info, err := file.Stat()
	require.NoError(t, err)
	headerBytes := make([]byte, checkpointHeaderSize)
	_, err = io.ReadFull(file, headerBytes)
	require.NoError(t, err)
	header, err := decodeCheckpointHeader(headerBytes, uint64(info.Size()))
	require.NoError(t, err)
	sources, err := readSourcesSection(sectionReader(file, header.Sections[checkpointSourcesSection]), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(41), sources["vortex/vllm-a"].LastAppliedSequence)

	restored, restoredIndex, _ := newTestPool(t, 16)
	restored.adapter = &sourceEndpointAdapter{}
	ok, err := restored.RestoreCheckpoint(path, testFingerprint)
	require.NoError(t, err)
	require.True(t, ok)

	source, ok := restored.resumeSource("vortex/vllm-a", "tcp://10.0.0.1:5559", "10.0.0.1:8000", intPtr(2))
	require.True(t, ok)
	require.Equal(t, uint64(41), source.LastAppliedSequence)
	require.Equal(t, checkpointEventDigest(msg.Topic, msg.Payload), source.EventDigest)
	requestKey, err := restoredIndex.GetRequestKey(t.Context(), 11)
	require.NoError(t, err)
	hits, err := restoredIndex.Lookup(t.Context(), []kvblock.BlockHash{requestKey}, nil)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:8000", hits[requestKey][0].PodIdentifier)
	group, ok := restored.groupCatalog.Get("10.0.0.1:8000", 3)
	require.True(t, ok)
	require.Equal(t, 16, group.BlockSize)
	kept := restored.dedup.filterRemove(blockScope{
		podIdentifier: "10.0.0.1:8000", deviceTier: "gpu",
		groupIdx: noGroupIdx, dataParallelRank: 2,
	}, []uint64{11})
	require.Empty(t, kept)
}

func TestRestoreLatestCheckpointUsesNewestWriterSnapshot(t *testing.T) {
	directory := t.TempDir()
	older, olderIndex, _ := newTestPool(t, 16)
	require.NoError(t, olderIndex.Add(t.Context(),
		[]kvblock.BlockHash{11}, []kvblock.BlockHash{101},
		[]kvblock.PodEntry{{PodIdentifier: "older:8000", DeviceTier: "gpu"}},
	))
	olderResult, err := older.WriteCheckpointForWriter(directory, "epp-a", testFingerprint)
	require.NoError(t, err)

	newer, newerIndex, _ := newTestPool(t, 16)
	require.NoError(t, newerIndex.Add(t.Context(),
		[]kvblock.BlockHash{22}, []kvblock.BlockHash{202},
		[]kvblock.PodEntry{{PodIdentifier: "newer:8000", DeviceTier: "gpu"}},
	))
	newerResult, err := newer.WriteCheckpointForWriter(directory, "epp-b", testFingerprint)
	require.NoError(t, err)
	require.NotEqual(t, olderResult.Path, newerResult.Path)
	baseTime := time.Unix(1_700_000_000, 0)
	require.NoError(t, os.Chtimes(olderResult.Path, baseTime, baseTime))
	require.NoError(t, os.Chtimes(newerResult.Path, baseTime.Add(time.Second), baseTime.Add(time.Second)))

	restored, restoredIndex, _ := newTestPool(t, 16)
	path, ok, err := restored.RestoreLatestCheckpoint(directory, testFingerprint)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, newerResult.Path, path)
	hits, err := restoredIndex.Lookup(t.Context(), []kvblock.BlockHash{101, 202}, nil)
	require.NoError(t, err)
	require.NotContains(t, hits, kvblock.BlockHash(101))
	require.Equal(t, "newer:8000", hits[202][0].PodIdentifier)
}

func TestRestoreLatestCheckpointFallsBackFromCorruptWriterSnapshot(t *testing.T) {
	directory := t.TempDir()
	older, olderIndex, _ := newTestPool(t, 16)
	require.NoError(t, olderIndex.Add(t.Context(),
		[]kvblock.BlockHash{11}, []kvblock.BlockHash{101},
		[]kvblock.PodEntry{{PodIdentifier: "older:8000", DeviceTier: "gpu"}},
	))
	olderResult, err := older.WriteCheckpointForWriter(directory, "epp-a", testFingerprint)
	require.NoError(t, err)

	newer, _, _ := newTestPool(t, 16)
	newerResult, err := newer.WriteCheckpointForWriter(directory, "epp-b", testFingerprint)
	require.NoError(t, err)
	encoded, err := os.ReadFile(newerResult.Path)
	require.NoError(t, err)
	encoded[len(encoded)-1] ^= 0xff
	require.NoError(t, os.WriteFile(newerResult.Path, encoded, 0o600))
	baseTime := time.Unix(1_700_000_000, 0)
	require.NoError(t, os.Chtimes(olderResult.Path, baseTime, baseTime))
	require.NoError(t, os.Chtimes(newerResult.Path, baseTime.Add(time.Second), baseTime.Add(time.Second)))

	restored, restoredIndex, _ := newTestPool(t, 16)
	path, ok, err := restored.RestoreLatestCheckpoint(directory, testFingerprint)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, olderResult.Path, path)
	hits, err := restoredIndex.Lookup(t.Context(), []kvblock.BlockHash{101}, nil)
	require.NoError(t, err)
	require.Equal(t, "older:8000", hits[101][0].PodIdentifier)
}

func TestRestoreCheckpointRejectsCorruptState(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, nil, nil)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	_, err = pool.WriteCheckpoint(path, testFingerprint)
	require.NoError(t, err)

	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	encoded[len(encoded)-1] ^= 0xff
	require.NoError(t, os.WriteFile(path, encoded, 0o600))

	_, err = pool.RestoreCheckpoint(path, testFingerprint)
	require.ErrorContains(t, err, "checksum")
}

func TestCheckpointHeaderUsesContiguousSections(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, nil, nil)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	_, err = pool.WriteCheckpoint(path, testFingerprint)
	require.NoError(t, err)

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	info, err := file.Stat()
	require.NoError(t, err)
	headerBytes := make([]byte, checkpointHeaderSize)
	_, err = io.ReadFull(file, headerBytes)
	require.NoError(t, err)
	header, err := decodeCheckpointHeader(headerBytes, uint64(info.Size()))
	require.NoError(t, err)
	offset := uint64(checkpointHeaderSize)
	for _, section := range header.Sections {
		require.Equal(t, offset, section.Offset)
		offset += section.Length
	}
	require.Equal(t, uint64(info.Size()), offset)
	require.NoError(t, verifyCheckpointBody(file, uint64(info.Size()), header.BodyChecksum))
}

func TestCheckpointHeaderRejectsUnknownFlags(t *testing.T) {
	header := checkpointHeader{CreatedAt: time.Unix(1, 0)}
	offset := uint64(checkpointHeaderSize)
	for i := range header.Sections {
		header.Sections[i].Offset = offset
	}
	encoded := encodeCheckpointHeader(header)
	binary.LittleEndian.PutUint32(encoded[92:96], 1)

	_, err := decodeCheckpointHeader(encoded, offset)
	require.ErrorContains(t, err, "unsupported header flags")
}

func TestCheckpointSectionsRejectOversizedCounts(t *testing.T) {
	var encoded bytes.Buffer
	require.NoError(t, binary.Write(&encoded, binary.LittleEndian, uint32(0)))
	require.NoError(t, binary.Write(&encoded, binary.LittleEndian, uint32(0)))
	require.NoError(t, binary.Write(&encoded, binary.LittleEndian, uint64(1)))
	section := checkpointSection{Length: uint64(encoded.Len()), Count: 1}
	reader := io.NewSectionReader(bytes.NewReader(encoded.Bytes()), 0, int64(encoded.Len()))

	_, err := readDedupSection(reader, section)
	require.ErrorContains(t, err, "invalid dedup count")

	encoded.Reset()
	require.NoError(t, binary.Write(&encoded, binary.LittleEndian, uint64(1)))
	reader = io.NewSectionReader(bytes.NewReader(encoded.Bytes()), 0, int64(encoded.Len()))
	_, err = readGroupsSection(reader, 1)
	require.ErrorContains(t, err, "invalid cache-group count")

	reader = io.NewSectionReader(bytes.NewReader(encoded.Bytes()), 0, int64(encoded.Len()))
	_, err = readSourcesSection(reader, 1)
	require.ErrorContains(t, err, "invalid event-source count")
}

func TestRestoreCheckpointRejectsTruncatedHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	require.NoError(t, os.WriteFile(path, checkpointMagic[:], 0o600))
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, nil, nil)
	require.NoError(t, err)

	_, err = pool.RestoreCheckpoint(path, testFingerprint)
	require.ErrorContains(t, err, "invalid checkpoint size")
}

func TestRestoreCheckpointRejectsOversizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(int64(checkpointMaxFileSize+1)))
	require.NoError(t, file.Close())
	pool, _, _ := newTestPool(t, 16)

	_, err = pool.RestoreCheckpoint(path, testFingerprint)
	require.ErrorContains(t, err, "invalid checkpoint size")
}

func TestCheckpointRejectsInvalidGroupMetadata(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.groupCatalog.Learn("10.0.0.1:8000", 3, kvblock.GroupMetadata{
		Kind: string(KVCacheSpecKindMlaAttention), BlockSize: 0,
	})

	_, err := pool.WriteCheckpoint(filepath.Join(t.TempDir(), "index.checkpoint"), testFingerprint)
	require.ErrorContains(t, err, "cache-group metadata")
}

func TestQueuedSourceResetPreventsCheckpoint(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.appliedSources["vortex/vllm-a"] = checkpointSource{
		EventEndpoint: "tcp://events", ServingEndpoint: "10.0.0.1:8000",
		LastAppliedSequence: 7, EventDigest: checkpointEventDigest("kv@test", []byte("boundary")),
	}
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	pool.resetForSource("kv@", "vortex/vllm-a", "tcp://events", "10.0.0.1:8000", nil)

	_, err := pool.WriteCheckpoint(path, testFingerprint)
	require.ErrorContains(t, err, "unapplied mutations")
	reset := drainOne(t, pool)
	require.True(t, pool.processRawMessage(t.Context(), reset))
	_, err = pool.WriteCheckpoint(path, testFingerprint)
	require.NoError(t, err)
}

func TestSourceResetDoesNotRemoveNewTransportBoundary(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	oldGeneration := pool.registerSource("vortex/vllm-a")
	pool.appliedSources["vortex/vllm-a"] = checkpointSource{
		EventEndpoint: "tcp://old-events", ServingEndpoint: "10.0.0.1:8000",
		LastAppliedSequence: 7, EventDigest: checkpointEventDigest("kv@test", []byte("old")),
		Generation: oldGeneration,
	}
	pool.resetForSource("kv@", "vortex/vllm-a", "tcp://old-events", "10.0.0.1:8000", nil)
	newGeneration := pool.registerSource("vortex/vllm-a")
	pool.recordAppliedSource(&RawMessage{
		Topic: "kv@test", Sequence: 2, Payload: []byte("new"),
		EventSourceID: "vortex/vllm-a", EventEndpoint: "tcp://new-events",
		SourceEndpoint: "10.0.0.2:8000", sourceGeneration: newGeneration,
	})
	pool.recordAppliedSource(&RawMessage{
		Topic: "kv@test", Sequence: 8, Payload: []byte("late-old"),
		EventSourceID: "vortex/vllm-a", EventEndpoint: "tcp://old-events",
		SourceEndpoint: "10.0.0.1:8000", sourceGeneration: oldGeneration,
	})

	reset := drainOne(t, pool)
	require.True(t, pool.processRawMessage(t.Context(), reset))
	source, found := pool.checkpointSource("vortex/vllm-a")
	require.True(t, found)
	require.Equal(t, "tcp://new-events", source.EventEndpoint)
	require.Equal(t, uint64(2), source.LastAppliedSequence)
}

func TestCheckpointWaitsForEveryQueuedSourceReset(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.resetForSource("kv@", "vortex/vllm-a", "tcp://events-a", "10.0.0.1:8000", nil)
	pool.resetForSource("kv@", "vortex/vllm-a", "tcp://events-b", "10.0.0.2:8000", nil)
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	resets := []*RawMessage{drainOne(t, pool), drainOne(t, pool)}
	if resets[0].SourceEndpoint == "10.0.0.1:8000" {
		resets[0], resets[1] = resets[1], resets[0]
	}

	require.True(t, pool.processRawMessage(t.Context(), resets[0]))
	_, err := pool.WriteCheckpoint(path, testFingerprint)
	require.ErrorContains(t, err, "unapplied mutations")
	require.True(t, pool.processRawMessage(t.Context(), resets[1]))
	_, err = pool.WriteCheckpoint(path, testFingerprint)
	require.NoError(t, err)
}

func TestOlderResetDoesNotResolveNewFailure(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.resetForSource("kv@", "vortex/vllm-a", "tcp://events", "10.0.0.1:8000", nil)
	pool.markSourceInvalid("vortex/vllm-a")

	require.True(t, pool.processRawMessage(t.Context(), drainOne(t, pool)))
	_, err := pool.WriteCheckpoint(filepath.Join(t.TempDir(), "index.checkpoint"), testFingerprint)
	require.ErrorContains(t, err, "unapplied mutations")
}

func TestSourceResetRetriesAfterClearFailure(t *testing.T) {
	base, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	index := &clearFailOnceIndex{Index: base}
	pool, err := NewPool(DefaultConfig(), index, nil, &sourceEndpointAdapter{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	pool.Start(ctx)
	t.Cleanup(func() { pool.Shutdown(ctx) })
	pool.resetForSource("kv@", "vortex/vllm-a", "tcp://events", "10.0.0.1:8000", nil)

	require.Eventually(t, func() bool {
		pool.appliedSourcesMu.RLock()
		defer pool.appliedSourcesMu.RUnlock()
		_, invalid := pool.invalidSources["vortex/vllm-a"]
		return index.clearAttempts.Load() >= 2 && !invalid
	}, time.Second, time.Millisecond)
}

func TestPoolShutdownStopsSourceResetRetry(t *testing.T) {
	base, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	index := &clearFailingIndex{Index: base}
	pool, err := NewPool(DefaultConfig(), index, nil, &sourceEndpointAdapter{})
	require.NoError(t, err)
	pool.Start(t.Context())
	pool.resetForSource("kv@", "vortex/vllm-a", "tcp://events", "10.0.0.1:8000", nil)
	require.Eventually(t, func() bool {
		return index.clearAttempts.Load() != 0
	}, time.Second, time.Millisecond)

	shutdownDone := make(chan struct{})
	go func() {
		pool.Shutdown(t.Context())
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("pool shutdown waited for a failing reset")
	}
}

func TestRetiredSourceFailureDoesNotInvalidateCheckpoint(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.adapter = &failingAdapter{}
	generation := pool.registerSource("vortex/vllm-a")
	pool.retireSourceGeneration("vortex/vllm-a", generation)

	applied := pool.processRawMessage(t.Context(), &RawMessage{
		Payload: []byte("invalid"), SourceEndpoint: "10.0.0.1:8000",
		EventSourceID: "vortex/vllm-a", sourceGeneration: generation,
	})
	require.True(t, applied)
	pool.appliedSourcesMu.RLock()
	_, invalid := pool.invalidSources["vortex/vllm-a"]
	pool.appliedSourcesMu.RUnlock()
	require.False(t, invalid)
}

func TestRetiredSourceDoesNotRecordCheckpointBoundary(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	generation := pool.registerSource("vortex/vllm-a")
	pool.retireSourceGeneration("vortex/vllm-a", generation)

	pool.recordAppliedSource(&RawMessage{
		Topic: "kv@test", Sequence: 9, Payload: []byte("event"),
		EventSourceID: "vortex/vllm-a", EventEndpoint: "tcp://events",
		SourceEndpoint: "10.0.0.1:8000", sourceGeneration: generation,
	})
	_, found := pool.checkpointSource("vortex/vllm-a")
	require.False(t, found)
}

func TestCheckpointEventDigestIncludesTopic(t *testing.T) {
	payload := []byte("same payload")
	require.NotEqual(t,
		checkpointEventDigest("kv@model-a", payload),
		checkpointEventDigest("kv@model-b", payload),
	)
}

func TestFailedMessageDoesNotAdvanceAppliedSequence(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.adapter = &failingAdapter{}
	applied := pool.processRawMessage(t.Context(), &RawMessage{
		Sequence: 9, Payload: []byte("invalid"),
		SourceEndpoint: "10.0.0.1:8000",
		EventSourceID:  "vortex/vllm-a", EventEndpoint: "tcp://10.0.0.1:5557",
	})
	require.False(t, applied)
	_, found := pool.resumeSource(
		"vortex/vllm-a", "tcp://10.0.0.1:5557", "10.0.0.1:8000", nil,
	)
	require.False(t, found)
	_, err := pool.WriteCheckpoint(filepath.Join(t.TempDir(), "index.checkpoint"), testFingerprint)
	require.ErrorContains(t, err, "unapplied mutations")
}

func TestFailedMutationPreventsCheckpointAdvance(t *testing.T) {
	base, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	index := &addFailingCheckpointIndex{Index: base, snapshotter: base}
	tokenProcessor, err := kvblock.NewChunkedTokenDatabase(&kvblock.TokenProcessorConfig{
		BlockSizeTokens: 16, HashSeed: "test",
	})
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, tokenProcessor, &sourceEndpointAdapter{})
	require.NoError(t, err)
	message := &RawMessage{
		Topic: "kv@test", Sequence: 9, Payload: []byte{11},
		SourceEndpoint: "10.0.0.1:8000", EventSourceID: "vortex/vllm-a",
		EventEndpoint: "tcp://events",
	}

	require.False(t, pool.processRawMessage(t.Context(), message))
	_, found := pool.checkpointSource(message.EventSourceID)
	require.False(t, found)
	_, err = pool.WriteCheckpoint(filepath.Join(t.TempDir(), "index.checkpoint"), testFingerprint)
	require.ErrorContains(t, err, "unapplied mutations")
}

func TestFailedMutationRequestsSubscriberRecovery(t *testing.T) {
	base, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	index := &addFailingCheckpointIndex{Index: base, snapshotter: base}
	tokenProcessor, err := kvblock.NewChunkedTokenDatabase(&kvblock.TokenProcessorConfig{
		BlockSizeTokens: 16, HashSeed: "test",
	})
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, tokenProcessor, &sourceEndpointAdapter{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	pool.Start(ctx)
	t.Cleanup(func() { pool.Shutdown(ctx) })
	subscriber := newZMQSubscriber(
		pool, "vortex/vllm-a", "10.0.0.1:8000", "tcp://events", "tcp://replay", "kv@", nil, true,
	)
	subscriber.lastSeq = 8
	subscriber.hasLastSeq = true
	subscriber.addTask(ctx, "kv@test", 9, []byte{11})

	require.Eventually(t, func() bool {
		subscriber.recoveryMu.Lock()
		defer subscriber.recoveryMu.Unlock()
		return subscriber.recoveryRequired
	}, time.Second, time.Millisecond)
	subscriber.prepareRecovery()
	require.False(t, subscriber.hasLastSeq)
	require.Eventually(t, func() bool {
		pool.appliedSourcesMu.RLock()
		defer pool.appliedSourcesMu.RUnlock()
		_, invalid := pool.invalidSources[subscriber.podIdentifier]
		return !invalid
	}, time.Second, time.Millisecond)
}

func TestSubscriberRetirementResolvesPendingRecovery(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	subscriber := newZMQSubscriber(
		pool, "vortex/vllm-a", "10.0.0.1:8000", "tcp://events", "tcp://replay", "kv@", nil, true,
	)
	subscriber.requestRecovery()
	subscriber.retire(true)

	reset := drainOne(t, pool)
	require.Len(t, reset.resetVersions, 2)
	require.True(t, pool.processRawMessage(t.Context(), reset))
	_, err := pool.WriteCheckpoint(filepath.Join(t.TempDir(), "index.checkpoint"), testFingerprint)
	require.NoError(t, err)
}

func TestSubscriberDoesNotConnectWithPendingRecovery(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	subscriber := newZMQSubscriber(
		pool, "vortex/vllm-a", "10.0.0.1:8000", "tcp://events", "tcp://replay", "kv@", nil, true,
	)
	subscriber.requestRecovery()

	require.False(t, subscriber.installRunCancel(func() {}))
	subscriber.prepareRecovery()
	require.True(t, drainOne(t, pool).reset)
}

func TestRestoreCheckpointMustRunBeforePoolStart(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.Start(t.Context())
	t.Cleanup(func() { pool.Shutdown(t.Context()) })

	_, err := pool.RestoreCheckpoint(filepath.Join(t.TempDir(), "missing"), testFingerprint)
	require.ErrorContains(t, err, "before the event pool starts")
}

func TestSubscriberUsesOnlyMatchingRestoredSource(t *testing.T) {
	pool, err := NewPool(DefaultConfig(), nil, nil, nil)
	require.NoError(t, err)
	pool.appliedSources["vortex/vllm-a"] = checkpointSource{
		EventEndpoint: "tcp://10.0.0.1:5557", ServingEndpoint: "10.0.0.1:8000",
		LastAppliedSequence: 23, EventDigest: checkpointEventDigest("", []byte("last")),
	}

	matching := newZMQSubscriber(pool, "vortex/vllm-a", "10.0.0.1:8000",
		"tcp://10.0.0.1:5557", "tcp://10.0.0.1:5657", "kv@", nil, true)
	require.True(t, matching.hasResume)
	require.Equal(t, uint64(23), matching.lastSeq)

	nonMatching := newZMQSubscriber(pool, "vortex/vllm-a", "10.0.0.9:8000",
		"tcp://10.0.0.9:5557", "tcp://10.0.0.9:5657", "kv@", nil, true)
	require.False(t, nonMatching.hasResume)
	reset := drainOne(t, pool)
	require.True(t, reset.reset)
	require.Equal(t, "10.0.0.1:8000", reset.SourceEndpoint)
}

func TestSubscriberClearsRestoredSourceAfterServingEndpointReuse(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.adapter = &sourceEndpointAdapter{}
	oldMessage := &RawMessage{
		Topic: "kv@test", Sequence: 7, Payload: []byte{11},
		EventSourceID: "vortex/vllm-a", EventEndpoint: "tcp://old-events",
		SourceEndpoint: "10.0.0.1:8000",
	}
	require.True(t, pool.processRawMessage(t.Context(), oldMessage))
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	_, err := pool.WriteCheckpoint(path, testFingerprint)
	require.NoError(t, err)

	restored, restoredIndex, _ := newTestPool(t, 16)
	restored.adapter = &sourceEndpointAdapter{}
	ok, err := restored.RestoreCheckpoint(path, testFingerprint)
	require.NoError(t, err)
	require.True(t, ok)
	requestKey, err := restoredIndex.GetRequestKey(t.Context(), 11)
	require.NoError(t, err)
	hits, err := restoredIndex.Lookup(t.Context(), []kvblock.BlockHash{requestKey}, nil)
	require.NoError(t, err)
	require.Len(t, hits[requestKey], 1)

	subscriber := newZMQSubscriber(
		restored, "vortex/vllm-b", "10.0.0.1:8000",
		"tcp://new-events", "tcp://new-replay", "kv@", nil, true,
	)
	require.False(t, subscriber.hasResume)
	reset := drainOne(t, restored)
	require.True(t, reset.reset)
	require.Equal(t, "vortex/vllm-a", reset.EventSourceID)
	require.Equal(t, "10.0.0.1:8000", reset.SourceEndpoint)
	require.True(t, restored.processRawMessage(t.Context(), reset))

	hits, err = restoredIndex.Lookup(t.Context(), []kvblock.BlockHash{requestKey}, nil)
	require.NoError(t, err)
	require.Empty(t, hits[requestKey])
}

func TestSubscriberClearsOnlyReusedDataParallelRank(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	rank0, rank1 := 0, 1
	pool.appliedSources["vortex/vllm-a-rank-0"] = checkpointSource{
		EventEndpoint: "tcp://old-rank-0", ServingEndpoint: "10.0.0.1:8000",
		DataParallelRank: &rank0, LastAppliedSequence: 7,
		EventDigest: checkpointEventDigest("kv@test", []byte("rank-0")),
	}
	pool.appliedSources["vortex/vllm-a-rank-1"] = checkpointSource{
		EventEndpoint: "tcp://old-rank-1", ServingEndpoint: "10.0.0.1:8000",
		DataParallelRank: &rank1, LastAppliedSequence: 9,
		EventDigest: checkpointEventDigest("kv@test", []byte("rank-1")),
	}

	subscriber := newZMQSubscriber(
		pool, "vortex/vllm-b-rank-0", "10.0.0.1:8000",
		"tcp://new-rank-0", "tcp://new-replay", "kv@", &rank0, true,
	)
	require.False(t, subscriber.hasResume)
	reset := drainOne(t, pool)
	require.NotNil(t, reset.ResetDataParallelRank)
	require.Equal(t, rank0, *reset.ResetDataParallelRank)
	require.True(t, pool.processRawMessage(t.Context(), reset))
	_, found := pool.checkpointSource("vortex/vllm-a-rank-0")
	require.False(t, found)
	_, found = pool.checkpointSource("vortex/vllm-a-rank-1")
	require.True(t, found)
	for _, queue := range pool.queues {
		require.Zero(t, queue.Len())
	}
}

func TestSharedRestoredEndpointClearsBeforeEitherSourceReplays(t *testing.T) {
	pool, index, _ := newTestPool(t, 16)
	pool.adapter = &sourceEndpointAdapter{}
	pool.concurrency = 1
	for id, payload := range map[string]byte{
		"vortex/vllm-a": 11,
		"vortex/vllm-b": 12,
	} {
		pool.appliedSources[id] = checkpointSource{
			EventEndpoint: "tcp://old-events", ServingEndpoint: "10.0.0.1:8000",
			LastAppliedSequence: 7,
			EventDigest:         checkpointEventDigest("kv@test", []byte{payload}),
		}
	}

	first := newZMQSubscriber(
		pool, "vortex/vllm-a", "10.0.0.1:8000",
		"tcp://new-events-a", "tcp://new-replay-a", "kv@", nil, true,
	)
	require.False(t, first.hasResume)
	second := newZMQSubscriber(
		pool, "vortex/vllm-b", "10.0.0.1:8000",
		"tcp://new-events-b", "tcp://new-replay-b", "kv@", nil, true,
	)
	require.False(t, second.hasResume)
	require.Equal(t, 2, pool.queues[0].Len())

	for range 2 {
		reset := drainOne(t, pool)
		require.True(t, reset.reset)
		require.True(t, pool.processRawMessage(t.Context(), reset))
	}
	first.addTask(t.Context(), "kv@test", 0, []byte{11})
	second.addTask(t.Context(), "kv@test", 0, []byte{12})
	for range 2 {
		require.True(t, pool.processRawMessage(t.Context(), drainOne(t, pool)))
	}

	for _, engineKey := range []kvblock.BlockHash{11, 12} {
		_, err := index.GetRequestKey(t.Context(), engineKey)
		require.NoError(t, err)
	}
	_, found := pool.checkpointSource("vortex/vllm-a")
	require.True(t, found)
	_, found = pool.checkpointSource("vortex/vllm-b")
	require.True(t, found)
}

func TestCheckpointReplayValidatesBoundaryAndQueuesOnlyNewEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	replayEndpoint := checkpointReplayServer(t, ctx, []checkpointReplayMessage{
		{sequence: 7, payload: []byte("checkpoint-boundary")},
		{sequence: 8, payload: []byte("new-event")},
	})
	pool, err := NewPool(DefaultConfig(), nil, nil, nil)
	require.NoError(t, err)
	pool.appliedSources["vortex/vllm-a"] = checkpointSource{
		EventEndpoint: "tcp://events", ServingEndpoint: "10.0.0.1:8000",
		LastAppliedSequence: 7, EventDigest: checkpointEventDigest("kv@test", []byte("checkpoint-boundary")),
	}
	subscriber := newZMQSubscriber(pool, "vortex/vllm-a", "10.0.0.1:8000",
		"tcp://events", replayEndpoint, "kv@", nil, true)

	require.True(t, subscriber.requestReplay(ctx, 7, &subscriber.resumeDigest))
	require.Equal(t, uint64(8), subscriber.lastSeq)
	replayed := drainOne(t, pool)
	require.Equal(t, uint64(8), replayed.Sequence)
	require.Equal(t, []byte("new-event"), replayed.Payload)
	for _, queue := range pool.queues {
		require.Zero(t, queue.Len())
	}
}

func TestCheckpointReplayRejectsChangedBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	replayEndpoint := checkpointReplayServer(t, ctx, []checkpointReplayMessage{
		{sequence: 7, payload: []byte("new-publisher-epoch")},
	})
	pool, err := NewPool(DefaultConfig(), nil, nil, nil)
	require.NoError(t, err)
	pool.appliedSources["vortex/vllm-a"] = checkpointSource{
		EventEndpoint: "tcp://events", ServingEndpoint: "10.0.0.1:8000",
		LastAppliedSequence: 7, EventDigest: checkpointEventDigest("kv@test", []byte("checkpoint-boundary")),
	}
	subscriber := newZMQSubscriber(pool, "vortex/vllm-a", "10.0.0.1:8000",
		"tcp://events", replayEndpoint, "kv@", nil, true)

	require.False(t, subscriber.requestReplay(ctx, 7, &subscriber.resumeDigest))
	reset := drainOne(t, pool)
	require.True(t, reset.reset)
	require.Equal(t, "vortex/vllm-a", reset.EventSourceID)
}

func TestCheckpointReplayKeepsRestoredStateAfterTransientFailure(t *testing.T) {
	pool, err := NewPool(DefaultConfig(), nil, nil, nil)
	require.NoError(t, err)
	digest := checkpointEventDigest("kv@test", []byte("checkpoint-boundary"))
	pool.appliedSources["vortex/vllm-a"] = checkpointSource{
		EventEndpoint: "tcp://events", ServingEndpoint: "10.0.0.1:8000",
		LastAppliedSequence: 7, EventDigest: digest,
	}
	subscriber := newZMQSubscriber(pool, "vortex/vllm-a", "10.0.0.1:8000",
		"tcp://events", "tcp://replay", "kv@", nil, true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.False(t, subscriber.requestReplay(ctx, 7, &subscriber.resumeDigest))
	require.True(t, subscriber.hasResume)
	_, found := pool.checkpointSource("vortex/vllm-a")
	require.True(t, found)
	for _, queue := range pool.queues {
		require.Zero(t, queue.Len())
	}
}

type checkpointReplayMessage struct {
	sequence uint64
	payload  []byte
}

type addFailingCheckpointIndex struct {
	kvblock.Index
	snapshotter kvblock.Snapshotter
}

type clearFailOnceIndex struct {
	kvblock.Index
	clearAttempts atomic.Int32
}

type clearFailingIndex struct {
	kvblock.Index
	clearAttempts atomic.Int32
}

func (i *clearFailOnceIndex) Clear(ctx context.Context, podIdentifier string) error {
	if i.clearAttempts.Add(1) == 1 {
		return errors.New("clear failed")
	}
	return i.Index.Clear(ctx, podIdentifier)
}

func (i *clearFailingIndex) Clear(context.Context, string) error {
	i.clearAttempts.Add(1)
	return errors.New("clear failed")
}

func (i *addFailingCheckpointIndex) Add(
	context.Context,
	[]kvblock.BlockHash,
	[]kvblock.BlockHash,
	[]kvblock.PodEntry,
) error {
	return errors.New("add failed")
}

func (i *addFailingCheckpointIndex) WriteSnapshot(dst io.Writer) error {
	return i.snapshotter.WriteSnapshot(dst)
}

func (i *addFailingCheckpointIndex) RestoreSnapshot(src io.Reader) error {
	return i.snapshotter.RestoreSnapshot(src)
}

func checkpointReplayServer(
	t *testing.T,
	ctx context.Context,
	messages []checkpointReplayMessage,
) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := fmt.Sprintf("tcp://%s", listener.Addr().String())
	require.NoError(t, listener.Close())
	router := zmq4.NewRouter(ctx)
	require.NoError(t, router.Listen(endpoint))
	t.Cleanup(func() { _ = router.Close() })

	go func() {
		request, receiveErr := router.Recv()
		if receiveErr != nil || len(request.Frames) != 3 {
			return
		}
		clientID := request.Frames[0]
		startSequence := binary.BigEndian.Uint64(request.Frames[2])
		for _, message := range messages {
			if message.sequence < startSequence {
				continue
			}
			sequence := make([]byte, 8)
			binary.BigEndian.PutUint64(sequence, message.sequence)
			if sendErr := router.Send(zmq4.NewMsgFrom(
				clientID, []byte{}, []byte("kv@test"), sequence, message.payload,
			)); sendErr != nil {
				return
			}
		}
		terminalSequence := make([]byte, 8)
		binary.BigEndian.PutUint64(terminalSequence, ^uint64(0))
		_ = router.Send(zmq4.NewMsgFrom(
			clientID, []byte{}, []byte{}, terminalSequence, []byte{},
		))
	}()
	return endpoint
}

func intPtr(value int) *int {
	return &value
}
