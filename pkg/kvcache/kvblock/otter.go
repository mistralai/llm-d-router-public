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

// OtterIndexConfig holds the configuration for the OtterIndex.
type OtterIndexConfig struct {
	// Size is the capacity target for each key store. Maintenance may
	// temporarily exceed it; it bounds key count rather than bytes.
	Size int `json:"size"`
	// PodCacheSize is the maximum number of pod entries per key.
	// A non-positive value selects defaultPodsPerKey.
	PodCacheSize int `json:"podCacheSize"`
}

func DefaultOtterIndexConfig() *OtterIndexConfig {
	return &OtterIndexConfig{
		Size:         defaultInMemoryIndexSize,
		PodCacheSize: defaultPodsPerKey,
	}
}

// OtterIndex uses Otter for both location and engine-key translation storage.
// The embedded implementation supplies indexing, traversal, and Add/Evict ordering.
type OtterIndex struct {
	*InMemoryIndex
}

var _ Index = &OtterIndex{}
var _ KeyWalker = &OtterIndex{}

func NewOtterIndex(cfg *OtterIndexConfig) (*OtterIndex, error) {
	if cfg == nil {
		cfg = DefaultOtterIndexConfig()
	}
	locations, err := newOtterStore[*PodCache](cfg.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Otter locations: %w", err)
	}
	engineKeys, err := newOtterStore[[]BlockHash](cfg.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Otter engine keys: %w", err)
	}
	return &OtterIndex{InMemoryIndex: newInMemoryIndex(locations, engineKeys, cfg.PodCacheSize)}, nil
}
