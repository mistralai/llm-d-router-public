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
	"fmt"
	"slices"

	"github.com/maypok86/otter/v2"
)

// otterStore buffers access history for eviction-policy maintenance.
// Membership changes are synchronous; policy maintenance may lag behind them.
type otterStore[V any] struct {
	cache *otter.Cache[BlockHash, V]
}

func newOtterStore[V any](size int) (*otterStore[V], error) {
	if size <= 0 {
		return nil, fmt.Errorf("otter store size must be positive: %d", size)
	}
	cache, err := otter.New(&otter.Options[BlockHash, V]{MaximumSize: size})
	if err != nil {
		return nil, err
	}
	return &otterStore[V]{cache: cache}, nil
}

func (s *otterStore[V]) Get(key BlockHash) (V, bool) {
	return s.cache.GetIfPresent(key)
}

func (s *otterStore[V]) Peek(key BlockHash) (V, bool) {
	entry, found := s.cache.GetEntryQuietly(key)
	return entry.Value, found
}

func (s *otterStore[V]) PeekInto(keys []BlockHash, out []V) []V {
	out = out[:0]
	for _, key := range keys {
		pc, _ := s.Peek(key)
		out = append(out, pc)
	}
	return out
}

func (s *otterStore[V]) Promote(keys []BlockHash) {
	for _, key := range keys {
		s.cache.GetIfPresent(key)
	}
}

func (s *otterStore[V]) LoadOrStore(key BlockHash, value V) (actual V, loaded bool) {
	actual, inserted := s.cache.SetIfAbsent(key, value)
	return actual, !inserted
}

func (s *otterStore[V]) Put(key BlockHash, value V) {
	s.cache.Set(key, value)
}

func (s *otterStore[V]) Remove(key BlockHash) bool {
	_, removed := s.cache.Invalidate(key)
	return removed
}

func (s *otterStore[V]) Keys() []BlockHash {
	return slices.Collect(s.cache.Keys())
}
