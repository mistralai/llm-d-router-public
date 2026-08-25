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

package kvblock_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func TestInMemoryCheckpointRoundTrip(t *testing.T) {
	ctx := t.Context()
	source, err := NewInMemoryIndex(DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	confirmed := PodEntry{PodIdentifier: "vllm-a", DeviceTier: "gpu"}
	require.NoError(t, source.Add(ctx, []BlockHash{11}, []BlockHash{101}, []PodEntry{confirmed}))
	require.NoError(t, source.Add(ctx, nil, []BlockHash{202}, []PodEntry{{PodIdentifier: "vllm-b", Speculative: true}}))

	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	target, err := NewInMemoryIndex(DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	require.NoError(t, target.Restore(snapshot))

	entries, err := target.Lookup(ctx, []BlockHash{101, 202}, nil)
	require.NoError(t, err)
	require.Equal(t, []PodEntry{confirmed}, entries[101])
	require.Empty(t, entries[202])
	requestKey, err := target.GetRequestKey(ctx, 11)
	require.NoError(t, err)
	require.Equal(t, BlockHash(101), requestKey)
}
