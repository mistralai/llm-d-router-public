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

import "fmt"

// locationStore synchronizes request-key membership. Callers separately lock
// mutable PodCache values. Capacity eviction can detach a returned value.
type locationStore interface {
	Get(BlockHash) (*PodCache, bool)
	Peek(BlockHash) (*PodCache, bool)
	PeekInto([]BlockHash, []*PodCache) []*PodCache
	Promote([]BlockHash)
	LoadOrStore(BlockHash, *PodCache) (actual *PodCache, loaded bool)
	Remove(BlockHash) bool
	Keys() []BlockHash
}

func newLocationStore(kind string, size int) (locationStore, error) {
	if size <= 0 {
		return nil, fmt.Errorf("location store size must be positive: %d", size)
	}
	switch kind {
	case "", "lru":
		return newLRUStore(size)
	case "otter":
		return newOtterStore(size)
	default:
		return nil, fmt.Errorf("unknown location store: %q", kind)
	}
}
