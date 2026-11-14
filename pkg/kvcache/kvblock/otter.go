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
	"slices"
	"sync"

	"github.com/go-logr/logr"
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
	// mu orders Add's mapping and pod writes against removal of empty keys.
	// Lookups and walks do not acquire it.
	mu   sync.Mutex
	data *otterStore[*PodCache]
	// Values are complete translation slices, immutable after publication.
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

	// Hold o.mu across the engine->request mappings and the pod entry inserts,
	// also when engineKeys is nil. Evict and Clear re-check emptiness under o.mu
	// before removing a request key or an engine key mapping, so they cannot
	// remove one that this call has written but whose entries it has not
	// inserted yet.
	o.mu.Lock()
	defer o.mu.Unlock()

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
		podCache, found := o.data.Get(requestKey)
		if !found {
			newPodCache := &PodCache{capacity: o.podCacheSize}

			// Publish a populated cache so a concurrent lookup cannot see an
			// empty generation between insertion and the first entry write.
			newPodCache.addAll(records)
			podCache, found = o.data.LoadOrStore(requestKey, newPodCache)
		}

		if found {
			podCache.addAll(records)
		}

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
		// Resolve each engine key's request keys and evict the pod entries.
		// Do not hold mu here: evictPodsFromRequestKey takes it to remove an
		// emptied request key.
		groups := make([][]BlockHash, len(keys))
		for i, key := range keys {
			rks, found := o.engineToRequestKeys.Peek(key)
			if !found {
				traceLogger.Info("engineKey not found in mapping, nothing to evict", "engineKey", key)
				continue
			}
			groups[i] = rks
			for _, rk := range rks {
				o.evictPodsFromRequestKey(rk, key, entries, traceLogger)
			}
		}

		// One lock hold covers every engine key's allEmpty check and mapping
		// removal, keeping the Add-vs-Evict TOCTOU protection while batching.
		o.mu.Lock()
		for i, key := range keys {
			if groups[i] == nil {
				continue
			}
			allEmpty := true
			for _, rk := range groups[i] {
				if pc, found := o.data.Peek(rk); found && pc != nil && pc.size() > 0 {
					allEmpty = false
					break
				}
			}
			if allEmpty {
				o.engineToRequestKeys.Remove(key)
			}
		}
		o.mu.Unlock()
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
	podCache, found := o.data.Peek(requestKey)
	if !found || podCache == nil {
		traceLogger.Info("requestKey not found in index, nothing to evict", "requestKey", requestKey, "engineKey", engineKey)
		return
	}

	isEmpty := podCache.removeAll(entries)

	traceLogger.Info("evicted pods from key", "requestKey", requestKey, "engineKey", engineKey, "pods", entries)

	if !isEmpty {
		return
	}

	o.mu.Lock()
	removed := o.removeIfEmptyLocked(requestKey)
	o.mu.Unlock()
	if removed && traceLogger.Enabled() {
		traceLogger.Info("removed requestKey from index as no pods remain", "requestKey", requestKey)
	}
}

// removeIfEmptyLocked removes requestKey from the index if its cache holds no
// entries, and reports whether it did. The caller holds o.mu, so an Add that
// already fetched this cache has finished its write before the check.
func (o *OtterIndex) removeIfEmptyLocked(requestKey BlockHash) bool {
	podCache, found := o.data.Peek(requestKey)
	if !found || podCache == nil {
		return false
	}

	podCache.mu.Lock()
	defer podCache.mu.Unlock()
	if len(podCache.entries) != 0 {
		return false
	}
	o.data.Remove(requestKey)
	return true
}

// Clear removes every entry for the pod from the index, across all device tiers.
// O(N) over the index for a pod it has held, and O(1) for a pod it never held,
// such as one whose first snapshot is about to load. It removes the request keys
// it empties under one o.mu hold per chunk of keys, re-checking each key's
// emptiness under the lock. Lookup does not take o.mu, so Clear does not stall Lookup.
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

	var emptied []BlockHash
	for _, requestKey := range o.data.Keys() {
		// Peek so a clear does not record accesses on keys it scans.
		podCache, found := o.data.Peek(requestKey)
		if !found || podCache == nil {
			continue
		}

		if matched := podCache.matching(podIdentifier, dataParallelRank); len(matched) > 0 && podCache.removeAll(matched) {
			emptied = append(emptied, requestKey)
		}
	}

	// One o.mu hold per chunk bounds how long Clear blocks Add.
	const clearChunkSize = 1024
	for chunk := range slices.Chunk(emptied, clearChunkSize) {
		o.mu.Lock()
		for _, requestKey := range chunk {
			o.removeIfEmptyLocked(requestKey)
		}
		o.mu.Unlock()
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
