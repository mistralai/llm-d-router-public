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
	"slices"

	"github.com/maypok86/otter/v2"
)

// otterStore buffers access history for eviction-policy maintenance.
// Membership changes are synchronous; policy maintenance may lag behind them.
type otterStore struct {
	cache *otter.Cache[BlockHash, *PodCache]
}

func newOtterStore(size int) (*otterStore, error) {
	cache, err := otter.New(&otter.Options[BlockHash, *PodCache]{MaximumSize: size})
	if err != nil {
		return nil, err
	}
	return &otterStore{cache: cache}, nil
}

func (s *otterStore) Get(key BlockHash) (*PodCache, bool) {
	return s.cache.GetIfPresent(key)
}

func (s *otterStore) Peek(key BlockHash) (*PodCache, bool) {
	entry, found := s.cache.GetEntryQuietly(key)
	return entry.Value, found
}

func (s *otterStore) PeekInto(keys []BlockHash, out []*PodCache) []*PodCache {
	out = out[:0]
	for _, key := range keys {
		pc, _ := s.Peek(key)
		out = append(out, pc)
	}
	return out
}

func (s *otterStore) Promote(keys []BlockHash) {
	for _, key := range keys {
		s.cache.GetIfPresent(key)
	}
}

func (s *otterStore) LoadOrStore(key BlockHash, value *PodCache) (actual *PodCache, loaded bool) {
	actual, inserted := s.cache.SetIfAbsent(key, value)
	return actual, !inserted
}

func (s *otterStore) Remove(key BlockHash) bool {
	_, removed := s.cache.Invalidate(key)
	return removed
}

func (s *otterStore) Keys() []BlockHash {
	return slices.Collect(s.cache.Keys())
}
