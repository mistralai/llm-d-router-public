/*
Copyright 2025 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvblock_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// TestPodEntryJSONEncoding pins the JSON encoding of PodEntry because RedisIndex
// uses it as a hash field name (see encodeRedisPodField). Two EPP replicas only
// agree on which entry to evict if they encode an identical pod byte-for-byte,
// so an encoding change strands entries written by any replica still running the
// previous build for the length of a rolling upgrade.
//
// The non-DP expectation below is the encoding as it stood before PodEntry
// became data-parallel aware: DataParallelRank must stay absent, not null.
func TestPodEntryJSONEncoding(t *testing.T) {
	rank0, rank2 := 0, 2

	tests := []struct {
		name  string
		entry kvblock.PodEntry
		want  string
	}{
		{
			name:  "non-DP entry is unchanged by DP awareness",
			entry: kvblock.PodEntry{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu"},
			want:  `{"PodIdentifier":"10.0.0.1:8000","DeviceTier":"gpu","Speculative":false,"HasGroup":false,"GroupIdx":0}`,
		},
		{
			name:  "rank 0 is distinct from no rank",
			entry: kvblock.PodEntry{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu", DataParallelRank: &rank0},
			want:  `{"PodIdentifier":"10.0.0.1:8000","DeviceTier":"gpu","Speculative":false,"HasGroup":false,"GroupIdx":0,"DataParallelRank":0}`,
		},
		{
			name:  "non-zero rank",
			entry: kvblock.PodEntry{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu", DataParallelRank: &rank2},
			want:  `{"PodIdentifier":"10.0.0.1:8000","DeviceTier":"gpu","Speculative":false,"HasGroup":false,"GroupIdx":0,"DataParallelRank":2}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := json.Marshal(tt.entry)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(encoded))

			var decoded kvblock.PodEntry
			require.NoError(t, json.Unmarshal(encoded, &decoded))
			assert.Equal(t, tt.entry, decoded, "round-trip must preserve the entry")
		})
	}
}

// TestPodEntryStringIncludesDataParallelRank covers the debug representation.
// Unlike the JSON encoding this is log-only, so it is free to change shape.
func TestPodEntryStringIncludesDataParallelRank(t *testing.T) {
	rank := 3

	withoutRank := kvblock.PodEntry{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu"}
	assert.Equal(t, "10.0.0.1:8000@gpu", withoutRank.String())

	withRank := kvblock.PodEntry{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu", DataParallelRank: &rank}
	assert.Equal(t, "10.0.0.1:8000@gpu[dp=3]", withRank.String())
}
