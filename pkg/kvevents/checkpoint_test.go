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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func TestCheckpointBinaryCodecRoundTrip(t *testing.T) {
	slidingWindow := 4096
	payload := checkpointPayload{
		SchemaVersion:            checkpointSchemaVersion,
		CreatedAt:                time.Unix(1_789_800_000, 123).UTC(),
		ConfigurationFingerprint: "fingerprint",
		Index: kvblock.IndexSnapshot{
			Entries: []kvblock.IndexSnapshotEntry{
				{
					RequestKey: 101,
					Pods: []kvblock.PodEntry{
						{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu", HasGroup: true, GroupIdx: 3},
						{PodIdentifier: "10.0.0.2:8000", DeviceTier: "cpu"},
					},
				},
			},
			EngineMappings: []kvblock.EngineMappingSnapshot{
				{EngineKey: 11, RequestKeys: []kvblock.BlockHash{101, 102}},
			},
		},
		Dedup: []dedupSnapshotEntry{
			{
				PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu", GroupIdx: 3,
				DataParallelRank: 1, BlockHash: 11, Count: 2,
			},
		},
		Groups: []kvblock.GroupCatalogSnapshotEntry{
			{
				PodIdentifier: "10.0.0.1:8000", GroupID: 3,
				Metadata: kvblock.GroupMetadata{
					Kind: string(KVCacheSpecKindSlidingWindow), BlockSize: 128,
					SlidingWindowSize: &slidingWindow,
				},
			},
		},
		LastConsumedSequences: map[string]uint64{"tcp://10.0.0.1:5557": 41},
	}

	encoded, err := marshalCheckpoint(payload)
	require.NoError(t, err)
	require.True(t, hasBinaryCheckpointHeader(encoded))

	decoded, err := unmarshalCheckpoint(encoded)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
}

func TestCheckpointBinaryCodecReducesSnapshotSize(t *testing.T) {
	const entries = 10_000
	payload := checkpointPayload{
		SchemaVersion:            checkpointSchemaVersion,
		CreatedAt:                time.Unix(1_789_800_000, 0).UTC(),
		ConfigurationFingerprint: "fingerprint",
		Index: kvblock.IndexSnapshot{
			Entries:        make([]kvblock.IndexSnapshotEntry, 0, entries),
			EngineMappings: make([]kvblock.EngineMappingSnapshot, 0, entries),
		},
		Dedup:                 make([]dedupSnapshotEntry, 0, entries),
		LastConsumedSequences: map[string]uint64{"tcp://10.0.0.1:5557": 41},
	}
	for i := 0; i < entries; i++ {
		podIdentifier := fmt.Sprintf("10.0.0.%d:8000", i%80)
		requestKey := kvblock.BlockHash(0x9e3779b97f4a7c15 ^ uint64(i)*0x100000001b3)
		engineKey := kvblock.BlockHash(0xc2b2ae3d27d4eb4f ^ uint64(i)*0x9e3779b1)
		payload.Index.Entries = append(payload.Index.Entries, kvblock.IndexSnapshotEntry{
			RequestKey: requestKey,
			Pods: []kvblock.PodEntry{{
				PodIdentifier: podIdentifier,
				DeviceTier:    "gpu",
				HasGroup:      true,
				GroupIdx:      3,
			}},
		})
		payload.Index.EngineMappings = append(payload.Index.EngineMappings, kvblock.EngineMappingSnapshot{
			EngineKey: engineKey, RequestKeys: []kvblock.BlockHash{requestKey},
		})
		payload.Dedup = append(payload.Dedup, dedupSnapshotEntry{
			PodIdentifier: podIdentifier,
			DeviceTier:    "gpu",
			GroupIdx:      3,
			BlockHash:     uint64(engineKey),
			Count:         1,
		})
	}

	binaryData, err := marshalCheckpoint(payload)
	require.NoError(t, err)
	jsonData, err := marshalLegacyCheckpoint(payload)
	require.NoError(t, err)
	t.Logf("binary bytes=%d legacy JSON bytes=%d", len(binaryData), len(jsonData))
	require.Less(t, len(binaryData), len(jsonData)/3,
		"binary checkpoint should use less than one third of the legacy JSON size")
}

func TestCheckpointBinaryCodecReadsLegacyJSON(t *testing.T) {
	payload := checkpointPayload{
		SchemaVersion:            checkpointSchemaVersion,
		CreatedAt:                time.Unix(1_789_800_000, 0).UTC(),
		ConfigurationFingerprint: "fingerprint",
		LastConsumedSequences:    map[string]uint64{"source": 7},
	}
	encoded, err := marshalLegacyCheckpoint(payload)
	require.NoError(t, err)

	decoded, err := unmarshalCheckpoint(encoded)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
}

func TestCheckpointBinaryCodecRejectsCorruptionAndUnknownVersion(t *testing.T) {
	payload := checkpointPayload{
		SchemaVersion:            checkpointSchemaVersion,
		CreatedAt:                time.Unix(1_789_800_000, 0).UTC(),
		ConfigurationFingerprint: "fingerprint",
	}
	encoded, err := marshalCheckpoint(payload)
	require.NoError(t, err)

	corrupted := append([]byte(nil), encoded...)
	corrupted[len(corrupted)-1] ^= 1
	_, err = unmarshalCheckpoint(corrupted)
	require.ErrorContains(t, err, "checksum")

	unknownVersion := append([]byte(nil), encoded...)
	unknownVersion[len(checkpointBinaryMagic)]++
	_, err = unmarshalCheckpoint(unknownVersion)
	require.ErrorContains(t, err, "unsupported binary checkpoint version")

	_, err = unmarshalCheckpoint(append([]byte(checkpointBinaryMagic), checkpointBinaryVersion))
	require.ErrorContains(t, err, "truncated")
}

func marshalLegacyCheckpoint(payload checkpointPayload) ([]byte, error) {
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encodedPayload)
	return json.Marshal(checkpointEnvelope{
		Checksum: hex.EncodeToString(digest[:]),
		Payload:  encodedPayload,
	})
}

func TestCheckpointRestoresIndexDedupAndLastConsumedSequences(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, nil, nil)
	require.NoError(t, err)
	require.NoError(t, pool.EnableCheckpointing())
	entry := kvblock.PodEntry{PodIdentifier: "vllm-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(t.Context(), []kvblock.BlockHash{11}, []kvblock.BlockHash{101}, []kvblock.PodEntry{entry}))
	pool.dedup.trackStore(blockScope{podIdentifier: "vllm-a", deviceTier: "gpu", groupIdx: noGroupIdx, dataParallelRank: noDataParallelRank}, []uint64{11, 11})
	pool.lastConsumedSeq["tcp://10.0.0.1:5557"] = 41
	pool.groupCatalog.Learn("vllm-a", 3, kvblock.GroupMetadata{Kind: string(KVCacheSpecKindMlaAttention), BlockSize: 16})

	data, result, err := pool.MarshalCheckpoint("token-config")
	require.NoError(t, err)
	require.Equal(t, 1, result.Sources)
	require.NotEmpty(t, data)

	restoredIndex, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	restored, err := NewPool(DefaultConfig(), restoredIndex, nil, nil)
	require.NoError(t, err)
	require.NoError(t, restored.RestoreCheckpoint(data, "token-config"))
	require.Equal(t, uint64(41), restored.lastConsumedSeq["tcp://10.0.0.1:5557"])
	entries, err := restoredIndex.Lookup(t.Context(), []kvblock.BlockHash{101}, nil)
	require.NoError(t, err)
	require.Equal(t, []kvblock.PodEntry{entry}, entries[101])
	group, ok := restored.groupCatalog.Get("vllm-a", 3)
	require.True(t, ok)
	require.Equal(t, kvblock.GroupMetadata{Kind: string(KVCacheSpecKindMlaAttention), BlockSize: 16}, group)
	kept := restored.dedup.filterRemove(blockScope{podIdentifier: "vllm-a", deviceTier: "gpu", groupIdx: noGroupIdx, dataParallelRank: noDataParallelRank}, []uint64{11})
	require.Empty(t, kept)
}

func TestRestoreCheckpointRejectsMismatchAndCorruption(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, nil, nil)
	require.NoError(t, err)
	require.NoError(t, pool.EnableCheckpointing())
	data, _, err := pool.MarshalCheckpoint("expected")
	require.NoError(t, err)

	require.ErrorContains(t, pool.RestoreCheckpoint(data, "different"), "configuration")
	require.ErrorContains(t, pool.RestoreCheckpoint([]byte(`{"checksum":"bad","payload":{}}`), "expected"), "checksum")
}

func TestCheckpointRejectsReplayInProgress(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, nil, nil)
	require.NoError(t, err)
	require.NoError(t, pool.EnableCheckpointing())
	pool.markReplayInProgress("tcp://10.0.0.1:5557")
	_, _, err = pool.MarshalCheckpoint("config")
	require.ErrorContains(t, err, "replay is in progress")
}

func TestReplayResetDoesNotExposePartialReplayToCheckpoint(t *testing.T) {
	index, err := kvblock.NewInMemoryIndex(kvblock.DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	pool, err := NewPool(DefaultConfig(), index, nil, nil)
	require.NoError(t, err)
	require.NoError(t, pool.EnableCheckpointing())
	const streamID = "tcp://10.0.0.1:5557"
	pool.markReplayInProgress(streamID)

	pool.processRawMessage(t.Context(), &RawMessage{
		SourceEndpoint: "10.0.0.1:8000",
		StreamID:       streamID,
		reset:          true,
	})

	_, _, err = pool.MarshalCheckpoint("config")
	require.ErrorIs(t, err, ErrCheckpointReplayInProgress)

	pool.processRawMessage(t.Context(), &RawMessage{
		SourceEndpoint: "10.0.0.1:8000",
		StreamID:       streamID,
		reset:          true,
		forgetStream:   true,
	})
	_, _, err = pool.MarshalCheckpoint("config")
	require.NoError(t, err)
}

func TestSubscriberStartsAtRestoredLastConsumedSequence(t *testing.T) {
	pool, err := NewPool(DefaultConfig(), nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, pool.EnableCheckpointing())
	streamID := "pod-a\n10.0.0.1:8000\ntcp://events"
	pool.lastConsumedSeq[streamID] = 23
	subscriber := newZMQSubscriber(pool, "pod-a", "10.0.0.1:8000", "tcp://events", "tcp://replay", "kv@", true)
	require.True(t, subscriber.hasLastSeq)
	require.Equal(t, uint64(23), subscriber.lastSeq)
	require.Equal(t, streamID, subscriber.streamID)
}
