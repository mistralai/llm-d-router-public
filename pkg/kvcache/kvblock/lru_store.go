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
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

// lruStore is the request-key LRU behind one lock the index owns, so a read
// path can refresh the recency of every key it visited under a single
// acquisition instead of one per key.
type lruStore struct {
	mu  sync.RWMutex
	lru *simplelru.LRU[BlockHash, *PodCache]
}

func newLRUStore(size int) (*lruStore, error) {
	lru, err := simplelru.NewLRU[BlockHash, *PodCache](size, nil)
	if err != nil {
		return nil, err
	}
	return &lruStore{lru: lru}, nil
}

// Get returns the key's cache and marks it most recently used.
func (s *lruStore) Get(key BlockHash) (*PodCache, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Get(key)
}

// Peek returns the key's cache without touching recency.
func (s *lruStore) Peek(key BlockHash) (*PodCache, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.Peek(key)
}

// PeekInto appends each key's cache, or nil when absent, to out[:0] under one
// shared acquisition, without touching recency.
func (s *lruStore) PeekInto(keys []BlockHash, out []*PodCache) []*PodCache {
	out = out[:0]
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, key := range keys {
		pc, _ := s.lru.Peek(key)
		out = append(out, pc)
	}
	return out
}

// Promote marks keys most recently used in order, so the last key ends up
// the most recent, under one acquisition. Absent keys are skipped.
func (s *lruStore) Promote(keys []BlockHash) {
	if len(keys) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		s.lru.Get(key)
	}
}

func (s *lruStore) Add(key BlockHash, value *PodCache) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lru.Add(key, value)
}

// ContainsOrAdd reports whether key is present and, when it is not, adds
// value.
func (s *lruStore) ContainsOrAdd(key BlockHash, value *PodCache) (contains, evicted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lru.Contains(key) {
		return true, false
	}
	return false, s.lru.Add(key, value)
}

func (s *lruStore) Remove(key BlockHash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Remove(key)
}

func (s *lruStore) Keys() []BlockHash {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.Keys()
}

func (s *lruStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.Len()
}

// addEntries publishes populated caches. A shared store lock prevents capacity
// eviction during per-block mutation. TryLock avoids waiting for a PodCache
// while holding a store lock; the contended path waits outside that lock.
func (s *lruStore) addEntries(key BlockHash, records []EntryRef, capacity int) {
	for {
		s.mu.RLock()
		pc, found := s.lru.Peek(key)
		if !found {
			s.mu.RUnlock()
			candidate := &PodCache{capacity: capacity}
			candidate.addAllLocked(records)
			s.mu.Lock()
			_, found = s.lru.Peek(key)
			if !found {
				s.lru.Add(key, candidate)
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			continue
		}
		if !pc.mu.TryLock() {
			s.mu.RUnlock()
			pc.mu.Lock()
			pc.mu.Unlock()
			continue
		}
		pc.addAllLocked(records)
		pc.mu.Unlock()
		s.mu.RUnlock()
		// The value mutation is complete before exclusive recency maintenance.
		// A concurrent capacity eviction may already have removed this key.
		s.mu.Lock()
		s.lru.Get(key)
		s.mu.Unlock()
		return
	}
}

func (s *lruStore) removeEntries(key BlockHash, entries []PodEntry) {
	for {
		s.mu.RLock()
		pc, found := s.lru.Peek(key)
		if !found {
			s.mu.RUnlock()
			return
		}
		if !pc.mu.TryLock() {
			s.mu.RUnlock()
			pc.mu.Lock()
			pc.mu.Unlock()
			continue
		}
		empty := pc.removeAllLocked(entries)
		s.mu.RUnlock()
		if empty {
			s.mu.Lock()
			// Add cannot modify pc until its lock is released. Capacity eviction
			// may have replaced it while the store lock was reacquired.
			if current, ok := s.lru.Peek(key); ok && current == pc {
				s.lru.Remove(key)
			}
			s.mu.Unlock()
		}
		pc.mu.Unlock()
		return
	}
}
