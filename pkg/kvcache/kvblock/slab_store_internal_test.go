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

func TestSlabRunAllocationAcrossChunkBoundaries(t *testing.T) {
	const size, entryCap = 3, 32769
	store, err := newSlabStore(size, entryCap, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)

	heads := make(map[uint32]struct{})
	for _, capacity := range runCapacities(entryCap) {
		for range size {
			head, err := store.allocRun(capacity)
			require.NoError(t, err)
			_, duplicate := heads[head]
			assert.False(t, duplicate, "run %d reused while live", head)
			heads[head] = struct{}{}
		}
	}
}

func TestSlabRunReuse(t *testing.T) {
	store, err := newSlabStore(1, 10, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)

	head, err := store.allocRun(10)
	require.NoError(t, err)
	store.freeRun(head, 10)
	reused, err := store.allocRun(10)
	require.NoError(t, err)
	assert.Equal(t, head, reused)
}

func TestSlabReportsReferenceCapacityExhaustion(t *testing.T) {
	store, err := newSlabStore(1, 1, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)
	store.nextChunk = uint32(len(store.refChunks))
	_, err = store.allocRun(1)
	require.ErrorContains(t, err, "slab reference capacity exhausted")
}

func TestSlabAddPreservesEntriesOnReferenceExhaustion(t *testing.T) {
	store, err := newSlabStore(2, 2, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)
	first := newCompactEntryRef(1, 1, false, false, 0)
	second := newCompactEntryRef(2, 1, false, false, 0)
	require.NoError(t, store.add(1, []slabRef{first}))

	store.nextChunk = uint32(len(store.refChunks))
	err = store.add(1, []slabRef{second})
	require.ErrorContains(t, err, "slab reference capacity exhausted")

	n, found := store.peek(1)
	require.True(t, found)
	n.mu.Lock()
	assert.Equal(t, uint16(1), n.runLen)
	assert.Equal(t, first, store.refs(n.head, n.runCap)[0])
	n.mu.Unlock()
}

func TestSlabAcceptsConfigurationBeyondReferenceAddressSpace(t *testing.T) {
	_, err := newSlabStore(1<<20, 1<<15, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)
}
