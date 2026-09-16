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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlabRunAllocationAcrossChunkBoundaries(t *testing.T) {
	const size, entryCap = 3, 32769
	store, err := newSlabStore(size, entryCap, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)

	heads := make(map[runHead]struct{})
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

func TestSlabNewKeyAllocatesFinalRun(t *testing.T) {
	store, err := newSlabStore(1, 8, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)
	records := make([]slabRef, 8)
	for i := range records {
		records[i] = newCompactEntryRef(uint32(i+1), 1, false, false, 0) // #nosec G115 -- len(records) bounds i.
	}

	require.NoError(t, store.add(1, records))
	assert.Equal(t, uint32(1), store.nextChunk)
	assert.False(t, store.runBumps[1].initialized)
	assert.False(t, store.runBumps[2].initialized)
	assert.False(t, store.runBumps[4].initialized)
	n, found := store.peek(1)
	require.True(t, found)
	assert.Equal(t, uint16(8), n.runCap)
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

func TestSlabAddPreservesVictimOnReferenceExhaustion(t *testing.T) {
	store, err := newSlabStore(1, 2, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)
	first := newCompactEntryRef(1, 1, false, false, 0)
	second := newCompactEntryRef(2, 1, false, false, 0)
	require.NoError(t, store.add(1, []slabRef{first}))

	store.nextChunk = uint32(len(store.refChunks))
	err = store.add(2, []slabRef{first, second})
	require.ErrorContains(t, err, "slab reference capacity exhausted")

	_, found := store.peek(1)
	assert.True(t, found, "failed allocation removed the LRU victim")
	_, found = store.peek(2)
	assert.False(t, found, "failed allocation published the new key")
}

func TestInMemoryAddIsAtomicOnReferenceExhaustion(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 3, PodCacheSize: 2})
	require.NoError(t, err)
	oldRequestKey := BlockHash(9)
	oldEntry := PodEntry{PodIdentifier: "pod-old", DeviceTier: "gpu"}
	require.NoError(t, index.Add(context.Background(), []BlockHash{101}, []BlockHash{oldRequestKey}, []PodEntry{oldEntry}))

	head, err := index.data.allocRun(1)
	require.NoError(t, err)
	index.data.freeRun(head, 1)
	index.data.nextChunk = uint32(len(index.data.refChunks))
	index.data.runBumps[1].offset = slabChunkSize

	engineKeys := []BlockHash{101, 102}
	requestKeys := []BlockHash{1, 2}
	entry := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	err = index.Add(context.Background(), engineKeys, requestKeys, []PodEntry{entry})
	require.ErrorContains(t, err, "slab reference capacity exhausted")

	for _, key := range requestKeys {
		_, found := index.data.peek(key)
		assert.False(t, found, "failed batch published request key %d", key)
	}
	requestKey, err := index.GetRequestKey(context.Background(), engineKeys[0])
	require.NoError(t, err)
	assert.Equal(t, oldRequestKey, requestKey)
	_, found := index.engineToRequestKeys.Peek(engineKeys[1])
	assert.False(t, found, "failed batch published engine key %d", engineKeys[1])
	_, found = index.pods.ids[entry.PodIdentifier]
	assert.False(t, found, "failed batch interned its pod")
}

func TestInMemoryAddAndEngineEvictDoNotDeadlock(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	engineKey := BlockHash(101)
	oldRequestKey := BlockHash(1)
	newRequestKey := BlockHash(2)
	entry := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(context.Background(), []BlockHash{engineKey}, []BlockHash{oldRequestKey}, []PodEntry{entry}))

	n, found := index.data.peek(oldRequestKey)
	require.True(t, found)
	n.mu.Lock()
	nodeLocked := true
	defer func() {
		if nodeLocked {
			n.mu.Unlock()
		}
	}()

	evictDone := make(chan error, 1)
	go func() {
		evictDone <- index.Evict(context.Background(), engineKey, EngineKey, []PodEntry{entry})
	}()
	require.Eventually(t, func() bool {
		if index.data.mu.TryLock() {
			index.data.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)

	addDone := make(chan error, 1)
	go func() {
		addDone <- index.Add(context.Background(), []BlockHash{engineKey}, []BlockHash{newRequestKey}, []PodEntry{entry})
	}()
	require.Eventually(t, func() bool {
		if index.mu.TryLock() {
			index.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)

	n.mu.Unlock()
	nodeLocked = false
	for operation, done := range map[string]<-chan error{"add": addDone, "evict": evictDone} {
		select {
		case err := <-done:
			require.NoError(t, err, "%s returned an error", operation)
		case <-time.After(time.Second):
			t.Fatalf("%s did not complete", operation)
		}
	}

	requestKey, err := index.GetRequestKey(context.Background(), engineKey)
	require.NoError(t, err)
	assert.Equal(t, newRequestKey, requestKey)
}

func TestInMemoryAddRefreshesWithoutReferenceCapacity(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 1, PodCacheSize: 2})
	require.NoError(t, err)
	entry := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{1}, []PodEntry{entry}))

	index.data.nextChunk = uint32(len(index.data.refChunks))
	index.data.runBumps[1].offset = slabChunkSize
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{1}, []PodEntry{entry}))
}

func TestInMemoryAddReplacesFullRunWithoutReferenceCapacity(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 1, PodCacheSize: 1})
	require.NoError(t, err)
	first := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{1}, []PodEntry{first}))

	index.data.nextChunk = uint32(len(index.data.refChunks))
	index.data.runBumps[1].offset = slabChunkSize
	second := PodEntry{PodIdentifier: "pod-b", DeviceTier: "gpu"}
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{1}, []PodEntry{second}))

	entries, _, found := index.data.filteredEntries(1, nil, false)
	require.True(t, found)
	assert.Equal(t, []PodEntry{second}, entries)
}

func TestInMemoryAddUsesFreeRunAtReferenceCapacity(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 1, PodCacheSize: 1})
	require.NoError(t, err)
	head, err := index.data.allocRun(1)
	require.NoError(t, err)
	index.data.freeRun(head, 1)
	index.data.nextChunk = uint32(len(index.data.refChunks))
	index.data.runBumps[1].offset = slabChunkSize

	entry := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{1}, []PodEntry{entry}))
}

func TestInMemoryAddCountsDuplicateRequestKeyOnce(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 1, PodCacheSize: 1})
	require.NoError(t, err)
	head, err := index.data.allocRun(1)
	require.NoError(t, err)
	index.data.freeRun(head, 1)
	index.data.nextChunk = uint32(len(index.data.refChunks))
	index.data.runBumps[1].offset = slabChunkSize

	entry := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{1, 1}, []PodEntry{entry}))
}

func TestInMemoryAddDoesNotReserveRunForStableKey(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	entry := PodEntry{PodIdentifier: "pod-a", DeviceTier: "gpu"}
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{1}, []PodEntry{entry}))

	head, err := index.data.allocRun(1)
	require.NoError(t, err)
	index.data.freeRun(head, 1)
	index.data.nextChunk = uint32(len(index.data.refChunks))
	index.data.runBumps[1].offset = slabChunkSize

	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{1, 2}, []PodEntry{entry}))
}

func TestInMemoryAddRejectsBeforeEvictingLaterBatchKey(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 1, PodCacheSize: 8})
	require.NoError(t, err)
	entries := make([]PodEntry, 8)
	for i := range entries {
		entries[i] = PodEntry{PodIdentifier: fmt.Sprintf("pod-%d", i), DeviceTier: "gpu"}
	}
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{2}, entries))

	head, err := index.data.allocRun(1)
	require.NoError(t, err)
	index.data.freeRun(head, 1)
	index.data.nextChunk = uint32(len(index.data.refChunks))
	index.data.runBumps[1].offset = slabChunkSize

	err = index.Add(context.Background(), nil, []BlockHash{1, 2}, entries[:1])
	require.ErrorContains(t, err, "slab reference capacity exhausted")
	_, found := index.data.peek(1)
	assert.False(t, found)
	n, found := index.data.peek(2)
	require.True(t, found)
	assert.Equal(t, uint16(8), n.runCap)
	assert.Equal(t, uint16(8), n.runLen)
}

func TestInMemoryAddReservesRunForEvictedLaterBatchKey(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 8})
	require.NoError(t, err)
	entries := make([]PodEntry, 8)
	for i := range entries {
		entries[i] = PodEntry{PodIdentifier: fmt.Sprintf("pod-%d", i), DeviceTier: "gpu"}
	}
	require.NoError(t, index.Add(context.Background(), nil, []BlockHash{2, 3}, entries))

	head, err := index.data.allocRun(1)
	require.NoError(t, err)
	index.data.freeRun(head, 1)
	index.data.nextChunk = uint32(len(index.data.refChunks))
	index.data.runBumps[1].offset = slabChunkSize

	err = index.Add(context.Background(), nil, []BlockHash{1, 2}, entries[:1])
	require.ErrorContains(t, err, "slab reference capacity exhausted")
	_, found := index.data.peek(1)
	assert.False(t, found)
	for _, key := range []BlockHash{2, 3} {
		n, found := index.data.peek(key)
		require.True(t, found)
		assert.Equal(t, uint16(8), n.runCap)
		assert.Equal(t, uint16(8), n.runLen)
	}
}

func TestSlabCapacityCheckUsesAllNonzeroChunkOffsets(t *testing.T) {
	store, err := newSlabStore(slabChunkSize, 1, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)
	store.nextChunk = uint32(len(store.refChunks) - 1)
	keys := make([]BlockHash, slabChunkSize)
	for i := range keys {
		keys[i] = BlockHash(uint64(i) + 1) // #nosec G115 -- len(keys) bounds i.
	}

	store.mu.Lock()
	err = store.ensureAddCapacityLocked(keys, []PodEntry{{PodIdentifier: "pod-a", DeviceTier: "gpu"}})
	store.mu.Unlock()
	require.NoError(t, err)
}

func TestSlabAcceptsConfigurationBeyondReferenceAddressSpace(t *testing.T) {
	_, err := newSlabStore(1<<20, 1<<15, newInterner(maxInternedPods), newInterner(maxInternedTiers))
	require.NoError(t, err)
}
