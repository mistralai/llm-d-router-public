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

package kvblock

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testClock(t *testing.T, size int) *clockEngineKeys {
	t.Helper()
	s, err := newClockEngineKeys(size)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func TestConcurrentClockValues(t *testing.T) {
	s := testClock(t, 4)
	input := []BlockHash{10, 11}
	s.Put(1, input)
	input[0] = 99
	old, found := s.Get(1)
	require.True(t, found)
	require.Equal(t, []BlockHash{10, 11}, old)
	identity, _ := s.state.entries.Load(BlockHash(1))
	s.Put(1, []BlockHash{10, 11})
	unchanged, _ := s.state.entries.Load(BlockHash(1))
	require.Same(t, identity, unchanged)
	s.Put(1, []BlockHash{20, 21})
	require.Equal(t, []BlockHash{10, 11}, old)
	require.False(t, s.state.entries.CompareAndDelete(BlockHash(1), identity), "a stale eviction must preserve a replacement")
	got, found := s.Peek(1)
	require.True(t, found)
	require.Equal(t, []BlockHash{20, 21}, got)
	require.True(t, s.Remove(1))
	require.False(t, s.Remove(1))
	s.wait()
	require.Zero(t, s.Len())
	require.Zero(t, s.state.reserved.Load())
}

func TestConcurrentClockSecondChance(t *testing.T) {
	s := testClock(t, 3)
	for key := BlockHash(1); key <= 3; key++ {
		s.Put(key, []BlockHash{key + 100})
	}
	s.wait()
	// Model a completed sweep, then a parent lookup of the first key.
	s.state.entries.Range(func(_, value any) bool { value.(*clockEntry).slot.used.Store(false); return true })
	_, found := s.Get(1)
	require.True(t, found)
	_, found = s.Peek(2)
	require.True(t, found)
	s.Put(4, []BlockHash{104})
	s.wait()
	require.True(t, s.Contains(1))
	require.False(t, s.Contains(2), "Peek must not grant a second chance")
	require.True(t, s.Contains(4))
	require.Equal(t, 3, s.Len())
	require.EqualValues(t, 3, s.state.reserved.Load())
}

func TestConcurrentClockChurn(t *testing.T) {
	s := testClock(t, 17)
	errors := make(chan string, 8)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range 1500 {
				key := BlockHash((n*7 + worker*3) % 37)
				value := BlockHash(n*8 + worker)
				s.Put(key, []BlockHash{value, value + 1})
				if got, ok := s.Get(key); ok && (len(got) != 2 || got[1] != got[0]+1) {
					errors <- "partially published mapping"
					return
				}
				if s.state.reserved.Load() > 17 {
					errors <- "capacity exceeded"
					return
				}
				if n%3 == 0 {
					s.Remove(key)
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("CLOCK admission or maintenance stopped progressing")
	}
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	s.wait()
	actual := 0
	s.state.entries.Range(func(_, _ any) bool { actual++; return true })
	require.Equal(t, actual, s.Len())
	require.EqualValues(t, actual, s.state.reserved.Load())
}

func TestConcurrentClockCleanup(t *testing.T) {
	state := func() *clockState {
		s, err := newClockEngineKeys(2)
		require.NoError(t, err)
		s.Put(1, []BlockHash{2})
		return s.state
	}()
	deadline := time.After(10 * time.Second)
	for {
		runtime.GC()
		select {
		case <-state.done:
			return
		case <-deadline:
			t.Fatal("unreachable store retained its maintenance worker")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func testRecords(names ...string) []EntryRef {
	result := make([]EntryRef, len(names))
	for i, name := range names {
		result[i] = EntryRef{PodEntry: PodEntry{PodIdentifier: name, DeviceTier: "gpu"}}
	}
	return result
}

func TestConcurrentLocationLastRemoval(t *testing.T) {
	s, err := newLRUStore(8)
	require.NoError(t, err)
	for range 1000 {
		s.addEntries(1, testRecords("a"), 4)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); s.removeEntries(1, []PodEntry{{PodIdentifier: "a", DeviceTier: "gpu"}}) }()
		go func() { defer wg.Done(); s.addEntries(1, testRecords("b"), 4) }()
		wg.Wait()
		pc, found := s.Peek(1)
		require.True(t, found, "the independent location addition must survive")
		got, _ := pc.filteredEntries(nil)
		require.Equal(t, []PodEntry{{PodIdentifier: "b", DeviceTier: "gpu"}}, got)
		s.removeEntries(1, got)
	}
}

func TestConcurrentLocationIndependentKeys(t *testing.T) {
	s, err := newLRUStore(8)
	require.NoError(t, err)
	s.addEntries(1, testRecords("a"), 4)
	pc, _ := s.Peek(1)
	pc.mu.Lock()
	blocked := make(chan struct{})
	go func() { s.addEntries(1, testRecords("b"), 4); close(blocked) }()
	runtime.Gosched()
	independent := make(chan struct{})
	go func() { s.addEntries(2, testRecords("c"), 4); close(independent) }()
	select {
	case <-independent:
	case <-time.After(5 * time.Second):
		pc.mu.Unlock()
		t.Fatal("waiting for a block held the global store lock")
	}
	pc.mu.Unlock()
	<-blocked
}

func TestConcurrentLocationCapacity(t *testing.T) {
	s, err := newLRUStore(7)
	require.NoError(t, err)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			records := testRecords(fmt.Sprintf("pod-%d", worker))
			for n := range 1000 {
				key := BlockHash((n + worker) % 13)
				s.addEntries(key, records, 16)
				if n%3 == 0 {
					s.removeEntries(key, []PodEntry{records[0].PodEntry})
				}
			}
		}()
	}
	wg.Wait()
	require.LessOrEqual(t, s.Len(), 7)
	for _, key := range s.Keys() {
		pc, _ := s.Peek(key)
		require.Positive(t, pc.size(), "empty location cache was left published")
	}
}

func TestConcurrentIndexCleanup(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 32, PodCacheSize: 8})
	require.NoError(t, err)
	t.Cleanup(index.engineToRequestKeys.Close)
	for _, pair := range []struct{ engine, request []BlockHash }{
		{[]BlockHash{1, 2}, []BlockHash{101, 102}},
		{[]BlockHash{3, 4}, []BlockHash{103}},
		{[]BlockHash{5}, []BlockHash{104, 105}},
	} {
		gpu := []PodEntry{{PodIdentifier: "pod", DeviceTier: "gpu"}}
		cpu := []PodEntry{{PodIdentifier: "pod", DeviceTier: "cpu"}}
		require.NoError(t, index.Add(t.Context(), pair.engine, pair.request, gpu))
		require.NoError(t, index.Add(t.Context(), pair.engine, pair.request, cpu))
		for _, key := range pair.engine {
			require.NoError(t, index.Evict(t.Context(), key, EngineKey, gpu))
		}
		for _, key := range pair.request {
			pc, found := index.data.Peek(key)
			require.True(t, found)
			got, _ := pc.filteredEntries(nil)
			require.Equal(t, cpu, got)
		}
		for _, key := range pair.engine {
			require.NoError(t, index.Evict(t.Context(), key, EngineKey, cpu))
		}
		for _, key := range pair.request {
			_, found := index.data.Peek(key)
			require.False(t, found)
		}
		for _, key := range pair.engine {
			require.False(t, index.engineToRequestKeys.Contains(key))
		}
	}
}

func TestConcurrentIndexParallelAdds(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	t.Cleanup(index.engineToRequestKeys.Close)
	entries := []PodEntry{{PodIdentifier: "pod", DeviceTier: "gpu"}}
	require.NoError(t, index.Add(t.Context(), []BlockHash{101}, []BlockHash{1}, entries))
	pc, _ := index.data.Peek(1)
	pc.mu.Lock()
	blocked := make(chan error, 1)
	go func() { blocked <- index.Add(t.Context(), []BlockHash{101}, []BlockHash{1}, entries) }()
	// Wait until Add has acquired its publication guard and reached the block.
	deadline := time.Now().Add(5 * time.Second)
	for index.mu.TryLock() {
		index.mu.Unlock()
		if time.Now().After(deadline) {
			pc.mu.Unlock()
			t.Fatal("Add did not acquire its publication guard")
		}
		runtime.Gosched()
	}
	independent := make(chan error, 1)
	go func() { independent <- index.Add(t.Context(), []BlockHash{102}, []BlockHash{2}, entries) }()
	select {
	case err := <-independent:
		pc.mu.Unlock()
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		pc.mu.Unlock()
		t.Fatal("an unrelated Add waited for the blocked key")
	}
	require.NoError(t, <-blocked)
}

func TestConcurrentIndexAddAndEvict(t *testing.T) {
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	t.Cleanup(index.engineToRequestKeys.Close)
	a := []PodEntry{{PodIdentifier: "a", DeviceTier: "gpu"}}
	b := []PodEntry{{PodIdentifier: "b", DeviceTier: "gpu"}}
	for range 1000 {
		require.NoError(t, index.Add(t.Context(), []BlockHash{101}, []BlockHash{1}, a))
		results := make(chan error, 2)
		go func() { results <- index.Evict(t.Context(), 101, EngineKey, a) }()
		go func() { results <- index.Add(t.Context(), []BlockHash{101}, []BlockHash{1}, b) }()
		require.NoError(t, <-results)
		require.NoError(t, <-results)
		key, err := index.GetRequestKey(t.Context(), 101)
		require.NoError(t, err, "cleanup removed a live mapping")
		require.Equal(t, BlockHash(1), key)
		pc, found := index.data.Peek(1)
		require.True(t, found)
		got, _ := pc.filteredEntries(nil)
		require.Equal(t, b, got)
		require.NoError(t, index.Evict(t.Context(), 101, EngineKey, b))
	}
}
