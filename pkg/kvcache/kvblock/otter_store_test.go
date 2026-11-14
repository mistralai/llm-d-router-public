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

	"github.com/maypok86/otter/v2"
	"github.com/stretchr/testify/require"
)

// Delayed policy work must not delay membership changes or resurrect removed values.
func TestOtterStoreMembershipBeforeMaintenance(t *testing.T) {
	var pending []func()
	cache, err := otter.New(&otter.Options[BlockHash, *PodCache]{
		MaximumSize: 100,
		Executor: func(fn func()) {
			pending = append(pending, fn)
		},
	})
	require.NoError(t, err)
	store := &otterStore[*PodCache]{cache: cache}
	first := &PodCache{capacity: 1}
	second := &PodCache{capacity: 2}

	actual, loaded := store.LoadOrStore(1, first)
	require.False(t, loaded)
	require.Same(t, first, actual)
	actual, loaded = store.LoadOrStore(1, second)
	require.True(t, loaded)
	require.Same(t, first, actual)
	actual, found := store.Get(1)
	require.True(t, found)
	require.Same(t, first, actual)

	require.True(t, store.Remove(1))
	require.False(t, store.Remove(1))
	_, found = store.Peek(1)
	require.False(t, found)
	actual, loaded = store.LoadOrStore(1, second)
	require.False(t, loaded)
	require.Same(t, second, actual)

	for len(pending) > 0 {
		fn := pending[0]
		pending = pending[1:]
		fn()
	}
	cache.CleanUp()

	actual, found = store.Peek(1)
	require.True(t, found)
	require.Same(t, second, actual)
	require.Equal(t, []*PodCache{second, nil, second},
		store.PeekInto([]BlockHash{1, 2, 1}, make([]*PodCache, 5)))
	require.Equal(t, []BlockHash{1}, store.Keys())
}

func TestOtterEngineKeysReplaceAndRemoveMappings(t *testing.T) {
	store, err := newOtterStore[[]BlockHash](100)
	require.NoError(t, err)
	store.Put(1, []BlockHash{10, 11})
	previous, found := store.Peek(1)
	require.True(t, found)
	store.Put(1, []BlockHash{20, 21})
	current, found := store.Get(1)
	require.True(t, found)
	require.Equal(t, []BlockHash{20, 21}, current)
	require.Equal(t, []BlockHash{10, 11}, previous)
	require.True(t, store.Remove(1))
	store.cache.CleanUp()
	_, found = store.Peek(1)
	require.False(t, found)
}

func TestOtterIndexEnforcesCapacityAfterMaintenance(t *testing.T) {
	const size = 32
	index, err := NewOtterIndex(&OtterIndexConfig{Size: size})
	require.NoError(t, err)
	for key := range BlockHash(1000) {
		require.NoError(t, index.Add(t.Context(), []BlockHash{10000 + key}, []BlockHash{key + 1},
			[]PodEntry{{PodIdentifier: "pod-a", DeviceTier: "gpu"}}))
	}
	locations := index.data
	translations := index.engineToRequestKeys
	locations.cache.CleanUp()
	translations.cache.CleanUp()
	require.LessOrEqual(t, locations.cache.EstimatedSize(), size)
	require.LessOrEqual(t, translations.cache.EstimatedSize(), size)
	require.NotEmpty(t, locations.Keys())
	require.NotEmpty(t, translations.Keys())
}
