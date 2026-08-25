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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func TestCheckpointRestoresIndexDedupAndLastConsumedSequences(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool := NewPool(DefaultConfig(), index, nil, nil)
	entry := kvblock.PodEntry{PodIdentifier: "vllm-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(t.Context(), []kvblock.BlockHash{11}, []kvblock.BlockHash{101}, []kvblock.PodEntry{entry}))
	pool.dedup.trackStore(blockScope{podIdentifier: "vllm-a", deviceTier: "gpu", groupIdx: noGroupIdx, dataParallelRank: noDataParallelRank}, []uint64{11, 11})
	pool.lastConsumedSeq["tcp://10.0.0.1:5557"] = 41
	pool.lastConsumedSeq["tcp://10.0.0.2:5557"] = 72
	pool.groupCatalog.Learn("vllm-a", 3, kvblock.GroupMetadata{Kind: string(KVCacheSpecKindMlaAttention), BlockSize: 16})
	path := filepath.Join(t.TempDir(), "index.checkpoint")

	result, err := pool.WriteCheckpoint(path, "token-config")
	require.NoError(t, err)
	require.Equal(t, 2, result.Sources)

	restoredIndex, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	restored := NewPool(DefaultConfig(), restoredIndex, nil, nil)
	ok, err := restored.RestoreCheckpoint(path, "token-config")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(41), restored.lastConsumedSeq["tcp://10.0.0.1:5557"])
	require.Equal(t, uint64(72), restored.lastConsumedSeq["tcp://10.0.0.2:5557"])
	entries, err := restoredIndex.Lookup(t.Context(), []kvblock.BlockHash{101}, nil)
	require.NoError(t, err)
	require.Equal(t, []kvblock.PodEntry{entry}, entries[101])
	group, ok := restored.groupCatalog.Get("vllm-a", 3)
	require.True(t, ok)
	require.Equal(t, kvblock.GroupMetadata{Kind: string(KVCacheSpecKindMlaAttention), BlockSize: 16}, group)
	kept := restored.dedup.filterRemove(blockScope{podIdentifier: "vllm-a", deviceTier: "gpu", groupIdx: noGroupIdx, dataParallelRank: noDataParallelRank}, []uint64{11})
	require.Empty(t, kept)
}

func TestRestoreCheckpointValidatesBeforeReplacingLiveState(t *testing.T) {
	sourceIndex, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	source := NewPool(DefaultConfig(), sourceIndex, nil, nil)
	require.NoError(t, sourceIndex.Add(t.Context(), []kvblock.BlockHash{11}, []kvblock.BlockHash{101}, []kvblock.PodEntry{{PodIdentifier: "snapshot"}}))
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	_, err = source.WriteCheckpoint(path, "config")
	require.NoError(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var envelope checkpointEnvelope
	require.NoError(t, json.Unmarshal(data, &envelope))
	var payload checkpointPayload
	require.NoError(t, json.Unmarshal(envelope.Payload, &payload))
	payload.Dedup = append(payload.Dedup, dedupSnapshotEntry{PodIdentifier: "snapshot", BlockHash: 11, Count: -1})
	envelope.Payload, err = json.Marshal(payload)
	require.NoError(t, err)
	digest := sha256.Sum256(envelope.Payload)
	envelope.Checksum = hex.EncodeToString(digest[:])
	data, err = json.Marshal(envelope)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	targetIndex, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	target := NewPool(DefaultConfig(), targetIndex, nil, nil)
	require.NoError(t, targetIndex.Add(t.Context(), []kvblock.BlockHash{22}, []kvblock.BlockHash{202}, []kvblock.PodEntry{{PodIdentifier: "live"}}))
	_, err = target.RestoreCheckpoint(path, "config")
	require.ErrorContains(t, err, "dedup")
	entries, err := targetIndex.Lookup(t.Context(), []kvblock.BlockHash{202}, nil)
	require.NoError(t, err)
	require.Equal(t, "live", entries[202][0].PodIdentifier)
}

func TestRestoreCheckpointRejectsMismatchAndCorruption(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool := NewPool(DefaultConfig(), index, nil, nil)
	path := filepath.Join(t.TempDir(), "index.checkpoint")
	_, err = pool.WriteCheckpoint(path, "expected")
	require.NoError(t, err)

	_, err = pool.RestoreCheckpoint(path, "different")
	require.ErrorContains(t, err, "configuration")
	require.NoError(t, os.WriteFile(path, []byte(`{"checksum":"bad","payload":{}}`), 0o600))
	_, err = pool.RestoreCheckpoint(path, "expected")
	require.ErrorContains(t, err, "checksum")
}

func TestCheckpointRejectsReplayInProgress(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool := NewPool(DefaultConfig(), index, nil, nil)
	pool.markReplayInProgress("tcp://10.0.0.1:5557")
	_, err = pool.WriteCheckpoint(filepath.Join(t.TempDir(), "index.checkpoint"), "config")
	require.ErrorContains(t, err, "replay is in progress")
}

func TestSubscriberStartsAtRestoredLastConsumedSequence(t *testing.T) {
	pool := NewPool(DefaultConfig(), nil, nil, nil)
	pool.lastConsumedSeq["tcp://events"] = 23
	subscriber := newZMQSubscriber(pool, "pod-a", "10.0.0.1:8000", "tcp://events", "tcp://replay", "kv@", true)
	require.True(t, subscriber.hasLastSeq)
	require.Equal(t, uint64(23), subscriber.lastSeq)
	require.True(t, subscriber.hasLastLiveSeq)
	require.Equal(t, uint64(23), subscriber.lastLiveSeq)
}

func TestProcessRawMessageRecordsLastConsumedSequence(t *testing.T) {
	pool, _, _ := newTestPool(t, 16)
	pool.adapter = &sourceEndpointAdapter{}
	pool.processRawMessage(t.Context(), &RawMessage{
		Sequence: 19, Payload: []byte{1}, SourceEndpoint: "10.0.0.1:8000", StreamID: "tcp://events",
	})
	seq, ok := pool.LastConsumedSequence("tcp://events")
	require.True(t, ok)
	require.Equal(t, uint64(19), seq)
}
