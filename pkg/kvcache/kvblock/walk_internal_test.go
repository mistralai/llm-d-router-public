/*
Copyright 2026 The llm-d Authors.

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

package kvblock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalkKeysUnlocksAfterPanic(t *testing.T) {
	for _, store := range []string{"lru", "otter"} {
		t.Run(store, func(t *testing.T) { testWalkKeysUnlocksAfterPanic(t, store) })
	}
}

func testWalkKeysUnlocksAfterPanic(t *testing.T, store string) {
	t.Helper()
	ctx := t.Context()
	index := newMemoryIndexForTesting(t, store, 1, 2)
	keys := []BlockHash{1}
	first := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(ctx, nil, keys, []PodEntry{first}))
	pc, found := index.data.Peek(keys[0])
	require.True(t, found)

	require.PanicsWithValue(t, "visit failed", func() {
		_ = index.WalkKeys(ctx, keys, func(int, bool, []EntryRef) bool {
			panic("visit failed")
		})
	})
	// Check the lock directly so a leaked lock fails without blocking the test.
	require.True(t, pc.mu.TryLock(), "callback panic left the key locked")
	pc.mu.Unlock()

	second := PodEntry{PodIdentifier: "pod-b", DeviceTier: "cpu"}
	require.NoError(t, index.Add(ctx, nil, keys, []PodEntry{second}))
	visits := 0
	require.NoError(t, index.WalkKeys(ctx, keys, func(_ int, found bool, entries []EntryRef) bool {
		visits++
		assert.True(t, found)
		require.Len(t, entries, 2)
		assert.ElementsMatch(t, []PodEntry{first, second}, []PodEntry{entries[0].PodEntry, entries[1].PodEntry})
		return true
	}))
	assert.Equal(t, 1, visits)
}
