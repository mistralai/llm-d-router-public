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

	"github.com/go-logr/logr"
	"github.com/maypok86/otter/v2"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

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
type OtterIndex struct {
	data *otterStore[*PodCache]
	// Translations are immutable and capacity-bounded independently of locations.
	engineToRequestKeys *otterStore[[]BlockHash]
	podCacheSize        int
	// Ordinals remain assigned for the lifetime of this index.
	pods, tiers *interner
}

var _ Index = &OtterIndex{}
var _ KeyWalker = &OtterIndex{}
var _ DataParallelRankIndex = &OtterIndex{}

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
	podCacheSize := cfg.PodCacheSize
	if podCacheSize <= 0 {
		podCacheSize = defaultPodsPerKey
	}
	return &OtterIndex{
		data:                locations,
		engineToRequestKeys: engineKeys,
		podCacheSize:        podCacheSize,
		pods:                newInterner(maxInternedPods),
		tiers:               newInterner(maxInternedTiers),
	}, nil
}

// internRecords pairs each entry with its pod and tier ordinals. A batch is
// assigned all or nothing: when its new pods or tiers would exceed a cap, no
// ordinal is consumed and the error names the cap.
func (o *OtterIndex) internRecords(entries []PodEntry) ([]EntryRef, error) {
	o.pods.mu.Lock()
	defer o.pods.mu.Unlock()
	o.tiers.mu.Lock()
	defer o.tiers.mu.Unlock()

	if !o.pods.fitsLocked(func(yield func(string)) {
		for i := range entries {
			yield(entries[i].PodIdentifier)
		}
	}) {
		return nil, fmt.Errorf("%w: %d pod identifiers", errIndexCardinality, maxInternedPods)
	}
	if !o.tiers.fitsLocked(func(yield func(string)) {
		for i := range entries {
			yield(entries[i].DeviceTier)
		}
	}) {
		return nil, fmt.Errorf("%w: %d device tiers", errIndexCardinality, maxInternedTiers)
	}

	records := make([]EntryRef, len(entries))
	for i, entry := range entries {
		records[i] = EntryRef{
			PodEntry:    entry,
			PodOrdinal:  o.pods.internLocked(entry.PodIdentifier),
			TierOrdinal: o.tiers.internLocked(entry.DeviceTier),
		}
	}
	return records, nil
}

// Lookup receives a list of requestKeys and a set of pod identifiers,
// and retrieves the filtered pods associated with those keys.
// The filtering is done based on the pod identifiers provided.
// If the podIdentifierSet is empty, all pods are returned.
//
// It returns:
// 1. A map where the keys are those in (1) and the values are pod-identifiers.
// 2. An error if any occurred during the operation.
//
// For non-empty requestKeys, Lookup uses WalkKeys' cancellation checkpoints
// and recency-promotion rules. It retains Lookup's empty-input error.
func (o *OtterIndex) Lookup(ctx context.Context, requestKeys []BlockHash,
	podIdentifierSet sets.Set[string],
) (map[BlockHash][]PodEntry, error) {
	if len(requestKeys) == 0 {
		return nil, fmt.Errorf("no requestKeys provided for lookup")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.OtterIndex.Lookup")

	podsPerKey := make(map[BlockHash][]PodEntry)
	highestHitIdx := 0
	visited := 0
	// Every exit, cancellation included, refreshes what was read.
	defer func() { o.data.Promote(requestKeys[:visited]) }()

	for idx, requestKey := range requestKeys {
		if idx&cancellationCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		pods, found := o.data.Peek(requestKey)
		if !found {
			if traceLogger.Enabled() {
				traceLogger.Info("no pods found for key, cutting search", "key", requestKey)
			}
			break // early stop since prefix-chain breaks here
		}
		var filtered []PodEntry
		var total int
		if pods != nil {
			filtered, total = pods.filteredEntries(podIdentifierSet)
		}
		visited = idx + 1
		if len(filtered) == 0 {
			if traceLogger.Enabled() {
				traceLogger.Info("no pods found for key, cutting search", "key", requestKey, "unfiltered-pods", total)
			}
			break // early stop since prefix-chain breaks here
		}

		highestHitIdx = idx
		podsPerKey[requestKey] = filtered
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if traceLogger.Enabled() {
		traceLogger.Info("lookup completed", "highest-hit-index", highestHitIdx,
			"pods-per-key", podsPerKeyPrintHelper(podsPerKey))
	}

	return podsPerKey, nil
}

// Add adds a set of engineKeys/requestKeys and their associated pod entries to the index backend.
// If engineKeys is nil, only requestKey -> PodEntry mappings are created (no engineKey -> requestKey mapping).
// This is used for speculative entries where engine keys are not yet known.
// When engineKeys is non-nil, the mapping type is inferred from the ratio of array lengths.
func (o *OtterIndex) Add(ctx context.Context, engineKeys, requestKeys []BlockHash, entries []PodEntry) error {
	if len(requestKeys) == 0 || len(entries) == 0 {
		return fmt.Errorf("no keys or entries provided for adding to index")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.OtterIndex.Add")

	// Intern once per call, before anything is written: a rejected batch
	// leaves no mapping and no ordinal behind. The same records apply to
	// every request key.
	records, err := o.internRecords(entries)
	if err != nil {
		return err
	}

	// Build engine->request mappings when engine keys are provided.
	// The ratio of array lengths determines the mapping type:
	//   equal  (4 eng, 4 req) -> 1:1   E0->R0, E1->R1, ...
	//   many:1 (4 eng, 1 req) -> E0->R0, E1->R0, E2->R0, E3->R0
	//   1:many (1 eng, 4 req) -> E0->[R0, R1, R2, R3]
	if engineKeys != nil {
		mappings := engineToRequestMapping(engineKeys, requestKeys)
		for ek, rks := range mappings {
			o.engineToRequestKeys.Put(ek, rks)
		}
	}

	// Store requestKey -> PodCache mappings for all request keys.

	for _, requestKey := range requestKeys {
		o.data.cache.Compute(requestKey, func(podCache *PodCache, found bool) (*PodCache, otter.ComputeOp) {
			if !found {
				podCache = &PodCache{capacity: o.podCacheSize}
			}
			podCache.addAll(records)
			if !found {
				return podCache, otter.WriteOp
			}
			return podCache, otter.CancelOp
		})
		// Compute's CancelOp keeps the mutable value without policy work.
		o.data.Get(requestKey)

		if traceLogger.Enabled() {
			traceLogger.Info("added pods to key", "requestKey", requestKey, "pods", entries)
		}
	}

	return nil
}

// Evict removes keys and their associated pod entries from the index backend.
// keyType indicates whether the keys are EngineKeys (each resolved independently
// via the engine-to-request mapping) or RequestKeys (used directly).
// A non-nil error means the index is unchanged.
func (o *OtterIndex) Evict(ctx context.Context, keyType KeyType, keys []BlockHash, entries []PodEntry) error {
	if len(keys) == 0 || len(entries) == 0 {
		return fmt.Errorf("no keys or entries provided for eviction from index")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.OtterIndex.Evict")

	switch keyType {
	case EngineKey:
		for _, key := range keys {
			rks, found := o.engineToRequestKeys.Peek(key)
			if !found {
				traceLogger.Info("engineKey not found in mapping, nothing to evict", "engineKey", key)
				continue
			}
			for _, rk := range rks {
				o.evictPodsFromRequestKey(rk, key, entries, traceLogger)
			}
		}

		return nil
	case RequestKey:
		for _, key := range keys {
			o.evictPodsFromRequestKey(key, EmptyBlockHash, entries, traceLogger)
		}
		return nil
	default:
		return fmt.Errorf("unknown key type: %d", keyType)
	}
}

// evictPodsFromRequestKey removes the given pod entries from a single request key's cache.
// If the cache becomes empty, the request key is removed from the index.
func (o *OtterIndex) evictPodsFromRequestKey(requestKey, engineKey BlockHash, entries []PodEntry, traceLogger logr.Logger) {
	o.data.cache.ComputeIfPresent(requestKey, func(podCache *PodCache) (*PodCache, otter.ComputeOp) {
		if podCache.removeAll(entries) {
			return nil, otter.InvalidateOp
		}
		return podCache, otter.CancelOp
	})
	if traceLogger.Enabled() {
		traceLogger.Info("evicted pods from key", "requestKey", requestKey, "engineKey", engineKey, "pods", entries)
	}
}

// Clear removes every entry for the pod from the index, across all device tiers.
// O(N) over the index for a pod it has held, and O(1) for a pod it never held,
// such as one whose first snapshot is about to load. Each key's removal is atomic
// with concurrent additions to that key.
//
// Context cancellation does not interrupt Clear.
//
// The engineKey->requestKey mapping (engineToRequestKeys) is intentionally left
// untouched: it is capacity-bounded, self-heals when the pod re-Adds the same prefixes,
// and any stale mapping resolves to an emptied request key that correctly breaks
// the prefix chain in Lookup.
func (o *OtterIndex) Clear(ctx context.Context, podIdentifier string) error {
	return o.clear(ctx, podIdentifier, nil)
}

// ClearRank removes one worker's entries while preserving sibling ranks.
func (o *OtterIndex) ClearRank(ctx context.Context, podIdentifier string, dataParallelRank int) error {
	return o.clear(ctx, podIdentifier, &dataParallelRank)
}

func (o *OtterIndex) clear(ctx context.Context, podIdentifier string, dataParallelRank *int) error {
	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.OtterIndex.Clear")

	// A pod is interned by its first Add and never released.
	o.pods.mu.Lock()
	_, interned := o.pods.ids[podIdentifier]
	o.pods.mu.Unlock()
	if !interned {
		return nil
	}

	for _, requestKey := range o.data.Keys() {
		o.data.cache.ComputeIfPresent(requestKey, func(podCache *PodCache) (*PodCache, otter.ComputeOp) {
			if matched := podCache.matching(podIdentifier, dataParallelRank); len(matched) > 0 && podCache.removeAll(matched) {
				return nil, otter.InvalidateOp
			}
			return podCache, otter.CancelOp
		})
	}

	traceLogger.Info("cleared pod from index", "pod", podIdentifier, "dataParallelRank", dataParallelRank)
	return nil
}

// GetRequestKey returns the last request key (highest index in the chain) associated with the given engineKey.
// This is what Pool uses for parent hash resolution.
// Returns an error if the engineKey mapping is missing (e.g., already evicted).
func (o *OtterIndex) GetRequestKey(ctx context.Context, engineKey BlockHash) (BlockHash, error) {
	rks, found := o.engineToRequestKeys.Get(engineKey)
	if !found || len(rks) == 0 {
		return EmptyBlockHash, fmt.Errorf("engine key not found: %s", engineKey.String())
	}
	return rks[len(rks)-1], nil
}

// WalkKeys implements KeyWalker. Keys are peeked in batches, and each key's
// entries are visited under that key's own lock after the store lookup ends.
// The visited prefix is promoted in a deferred call, so every exit path
// records what was read.
func (o *OtterIndex) WalkKeys(ctx context.Context, requestKeys []BlockHash,
	visit func(pos int, found bool, entries []EntryRef) bool,
) error {
	visited := 0
	// Every exit, cancellation included, refreshes what was read.
	defer func() { o.data.Promote(requestKeys[:visited]) }()
	var buf [walkBatchMax]*PodCache
	batch := walkBatchMin
	for start := 0; start < len(requestKeys); {
		end := min(start+batch, len(requestKeys))
		for i, pc := range o.data.PeekInto(requestKeys[start:end], buf[:]) {
			pos := start + i
			if pos&cancellationCheckMask == 0 && ctx.Err() != nil {
				return ctx.Err()
			}
			if pc == nil {
				if !visit(pos, false, nil) {
					return ctx.Err()
				}
				continue
			}
			visited = pos + 1
			if !pc.visitEntries(pos, visit) {
				return ctx.Err()
			}
		}
		start = end
		batch = min(2*batch, walkBatchMax)
	}
	return ctx.Err()
}
