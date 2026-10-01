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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	lru "github.com/hashicorp/golang-lru/v2"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

const (
	// defaultInMemoryIndexSize caps the index by entry count, not memory: the
	// underlying LRU evicts on count and has no notion of byte cost. To size
	// Size against available memory, account for a compact record per cached
	// pod up to PodCacheSize, per-key map and LRU metadata, and slab slack.
	// CostAwareMemoryIndex tracks actual byte cost per entry and
	// evicts against a configured memory budget directly; prefer it when the
	// workload's per-entry size is hard to predict up front.
	defaultInMemoryIndexSize = 1e8
	defaultPodsPerKey        = 10 // number of pods per key
)

// InMemoryIndexConfig holds the configuration for the InMemoryIndex.
type InMemoryIndexConfig struct {
	// Size is the maximum number of keys that can be stored in the index. It
	// bounds entry count, not memory; see defaultInMemoryIndexSize for sizing
	// it against available memory.
	Size int `json:"size"`
	// PodCacheSize is the maximum number of pod entries per key.
	// A non-positive value selects defaultPodsPerKey. The maximum is 65535.
	PodCacheSize int `json:"podCacheSize"`
}

// DefaultInMemoryIndexConfig returns a default configuration for the InMemoryIndex.
func DefaultInMemoryIndexConfig() *InMemoryIndexConfig {
	return &InMemoryIndexConfig{
		Size:         defaultInMemoryIndexSize,
		PodCacheSize: defaultPodsPerKey,
	}
}

// NewInMemoryIndex creates a new InMemoryIndex instance.
func NewInMemoryIndex(cfg *InMemoryIndexConfig) (*InMemoryIndex, error) {
	if cfg == nil {
		cfg = DefaultInMemoryIndexConfig()
	}
	podCacheSize := cfg.PodCacheSize
	if podCacheSize <= 0 {
		podCacheSize = defaultPodsPerKey
	}

	pods := newInterner(maxInternedPods)
	tiers := newInterner(maxInternedTiers)
	cache, err := newSlabStore(cfg.Size, podCacheSize, pods, tiers)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize in-memory index: %w", err)
	}

	engineToRequestKeys, err := lru.New[BlockHash, []BlockHash](cfg.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize in-memory engine key map: %w", err)
	}

	return &InMemoryIndex{
		data:                cache,
		engineToRequestKeys: engineToRequestKeys,
		pods:                pods,
		tiers:               tiers,
		baseClears:          make(map[uint32]baseClear),
	}, nil
}

// cancellationCheckMask paces context-cancellation checks in loops over
// request keys: positions where idx&mask == 0 poll ctx.Err().
const cancellationCheckMask = 255

// maxInternedPods and maxInternedTiers cap the distinct pod identifiers and
// device tiers an index assigns ordinals to over its lifetime.
const (
	maxInternedPods  = 1 << 24
	maxInternedTiers = 1 << 12
)

// errIndexCardinality reports an Add whose entries would exceed a cap.
var errIndexCardinality = errors.New("index cardinality limit reached")

// InMemoryIndex keeps new records in memory and can read a mapped snapshot base.
type InMemoryIndex struct {
	// snapshotMu keeps mutations out while a snapshot is encoded or installed.
	snapshotMu sync.RWMutex
	// mutationMu keeps engine mappings and their request entries consistent.
	mutationMu sync.Mutex
	// viewMu keeps mapped bytes valid while an operation uses them.
	viewMu sync.RWMutex
	// baseStateMu protects the masks applied to immutable mapped records.
	baseStateMu sync.RWMutex
	// data holds the mapping of requestKeys to sets of pod identifiers.
	data *slabStore
	// engineToRequestKeys holds the mapping of engineKeys to requestKeys.
	engineToRequestKeys *lru.Cache[BlockHash, []BlockHash]
	// pods and tiers assign the ordinals compact records carry. Neither is
	// reclaimed when entries leave the index: pod identifiers are endpoint
	// address:port values, bounded by the pod network's address space, and
	// tiers are the engine-reported names. Each is capped, and an Add past
	// a cap fails rather than admitting an entry that cannot be matched.
	pods       *interner
	tiers      *interner
	base       *mappedSnapshot
	baseClears map[uint32]baseClear
}

var _ Index = &InMemoryIndex{}
var _ CompactKeyWalker = &InMemoryIndex{}

// PodCache is an opaque compatibility type. In-memory indexes manage per-key
// storage internally.
//
// Deprecated: Do not use this type in new code.
type PodCache struct{}

// internRecords pairs each entry with its pod and tier ordinals. A batch is
// assigned all or nothing: when its new pods or tiers would exceed a cap, no
// ordinal is consumed and the error names the cap.
func (m *InMemoryIndex) internRecords(entries []PodEntry) ([]slabRef, error) {
	m.pods.mu.Lock()
	defer m.pods.mu.Unlock()
	m.tiers.mu.Lock()
	defer m.tiers.mu.Unlock()

	if !m.pods.fitsLocked(func(yield func(string)) {
		for i := range entries {
			yield(entries[i].PodIdentifier)
		}
	}) {
		return nil, fmt.Errorf("%w: %d pod identifiers", errIndexCardinality, maxInternedPods)
	}
	if !m.tiers.fitsLocked(func(yield func(string)) {
		for i := range entries {
			yield(entries[i].DeviceTier)
		}
	}) {
		return nil, fmt.Errorf("%w: %d device tiers", errIndexCardinality, maxInternedTiers)
	}

	records := make([]slabRef, len(entries))
	for i, entry := range entries {
		records[i] = newSlabRef(entry,
			m.pods.internLocked(entry.PodIdentifier),
			m.tiers.internLocked(entry.DeviceTier))
	}
	return records, nil
}

// knownRecords encodes entries already known to the interners. Unknown names
// cannot match an indexed record and are omitted.
func (m *InMemoryIndex) knownRecords(entries []PodEntry, records []slabRef) []slabRef {
	m.pods.mu.Lock()
	defer m.pods.mu.Unlock()
	m.tiers.mu.Lock()
	defer m.tiers.mu.Unlock()

	for _, entry := range entries {
		pod, podFound := m.pods.ids[entry.PodIdentifier]
		tier, tierFound := m.tiers.ids[entry.DeviceTier]
		if podFound && tierFound {
			records = append(records, newSlabRef(entry, pod, tier))
		}
	}
	return records
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
func (m *InMemoryIndex) Lookup(ctx context.Context, requestKeys []BlockHash,
	podIdentifierSet sets.Set[string],
) (map[BlockHash][]PodEntry, error) {
	if len(requestKeys) == 0 {
		return nil, fmt.Errorf("no requestKeys provided for lookup")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.InMemoryIndex.Lookup")
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	m.baseStateMu.RLock()
	defer m.baseStateMu.RUnlock()

	podsPerKey := make(map[BlockHash][]PodEntry)
	filtered := podIdentifierSet.Len() > 0
	var allowed map[uint32]struct{}
	if filtered {
		allowed = make(map[uint32]struct{}, podIdentifierSet.Len())
		m.pods.mu.Lock()
		for pod := range podIdentifierSet {
			if ordinal, found := m.pods.ids[pod]; found {
				allowed[ordinal] = struct{}{}
			}
		}
		m.pods.mu.Unlock()
	}
	highestHitIdx := 0
	visited := 0
	refs := make([]CompactEntryRef, 0, int(m.data.entryCap))
	// Every exit, cancellation included, refreshes what was read.
	defer func() { m.data.promote(requestKeys[:visited]) }()

	for idx, requestKey := range requestKeys {
		if idx&cancellationCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		refs, found := m.mergedCompactEntriesLocked(requestKey, refs)
		if !found {
			if traceLogger.Enabled() {
				traceLogger.Info("key not found in index", "key", requestKey)
			}
			continue
		}
		visited = idx + 1
		highestHitIdx = idx
		entries := make([]PodEntry, 0, len(refs))
		for _, ref := range refs {
			if filtered {
				if _, ok := allowed[ref.PodOrdinal]; !ok {
					continue
				}
			}
			entries = append(entries, ref.entry(m.pods, m.tiers).PodEntry)
		}
		if len(entries) != 0 {
			podsPerKey[requestKey] = entries
		}
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
func (m *InMemoryIndex) Add(ctx context.Context, engineKeys, requestKeys []BlockHash, entries []PodEntry) error {
	if len(requestKeys) == 0 || len(entries) == 0 {
		return fmt.Errorf("no keys or entries provided for adding to index")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.InMemoryIndex.Add")
	m.snapshotMu.RLock()
	defer m.snapshotMu.RUnlock()
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()
	m.baseStateMu.Lock()
	defer m.baseStateMu.Unlock()

	storeLocked := !m.data.hasWorstCaseAddCapacity(len(requestKeys))
	if storeLocked {
		m.data.mu.Lock()
		if err := m.data.ensureAddCapacityLocked(requestKeys, entries); err != nil {
			m.data.mu.Unlock()
			return fmt.Errorf("index cannot add request keys: %w", err)
		}
		m.data.mu.Unlock()
	}

	// Intern once per call, before anything is written: a rejected batch
	// leaves no mapping and no ordinal behind. The same records apply to
	// every request key.
	records, err := m.internRecords(entries)
	if err != nil {
		return err
	}

	for _, requestKey := range requestKeys {
		if err := m.addRequestRecordsLocked(requestKey, records); err != nil {
			return fmt.Errorf("failed to add request key: %w", err)
		}

		if traceLogger.Enabled() {
			traceLogger.Info("added pods to key", "requestKey", requestKey, "pods", entries)
		}
	}
	if engineKeys != nil {
		mappings := engineToRequestMapping(engineKeys, requestKeys)
		for ek, rks := range mappings {
			m.addEngineMappingLocked(ek, rks)
		}
	}

	return nil
}

// Evict removes a key and its associated pod entries from the index backend.
// keyType indicates whether the key is an EngineKey (requires engine→request lookup)
// or a RequestKey (used directly for speculative entries without engineKey mapping).
func (m *InMemoryIndex) Evict(ctx context.Context, key BlockHash, keyType KeyType, entries []PodEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("no entries provided for eviction from index")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.InMemoryIndex.Evict")
	m.snapshotMu.RLock()
	defer m.snapshotMu.RUnlock()
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()
	m.baseStateMu.Lock()
	defer m.baseStateMu.Unlock()
	var recordBuffer [1]slabRef // KV removal events contain one pod entry.
	records := recordBuffer[:0]
	if len(entries) > len(recordBuffer) {
		records = make([]slabRef, 0, len(entries))
	}
	records = m.knownRecords(entries, records)

	switch keyType {
	case EngineKey:
		rks, found := m.requestKeysLocked(key, nil)
		if !found {
			traceLogger.Info("engineKey not found in mapping, nothing to evict", "engineKey", key)
			return nil
		}

		for _, rk := range rks {
			if err := m.evictPodsFromRequestKey(rk, key, records, entries, traceLogger); err != nil {
				return err
			}
		}

		allEmpty := true
		if m.base == nil {
			for _, rk := range rks {
				if m.data.size(rk) > 0 {
					allEmpty = false
					break
				}
			}
		} else {
			refs := make([]CompactEntryRef, 0, int(m.data.entryCap))
			for _, rk := range rks {
				refs, _ = m.mergedCompactEntriesLocked(rk, refs[:0])
				if len(refs) > 0 {
					allEmpty = false
					break
				}
			}
		}
		if allEmpty {
			m.engineToRequestKeys.Remove(key)
			if m.base != nil {
				delete(m.base.engineOffsets, key)
			}
		}
		return nil
	case RequestKey:
		return m.evictPodsFromRequestKey(key, EmptyBlockHash, records, entries, traceLogger)
	default:
		return fmt.Errorf("unknown key type: %d", keyType)
	}
}

// evictPodsFromRequestKey removes the given pod entries from a single request key's cache.
// If the cache becomes empty, the request key is removed from the index.
func (m *InMemoryIndex) evictPodsFromRequestKey(requestKey, engineKey BlockHash, records []slabRef,
	entries []PodEntry, traceLogger logr.Logger,
) error {
	baseFound := false
	if m.base != nil {
		_, baseFound = m.base.requestOffsets[requestKey]
	}
	if !baseFound {
		if !m.data.remove(requestKey, records) {
			traceLogger.Info("requestKey not found in index, nothing to evict", "requestKey", requestKey, "engineKey", engineKey)
			return nil
		}
		traceLogger.Info("evicted pods from key", "requestKey", requestKey, "engineKey", engineKey, "pods", entries)
		return nil
	}
	merged, found := m.mergedCompactEntriesLocked(requestKey, nil)
	if !found {
		traceLogger.Info("requestKey not found in index, nothing to evict", "requestKey", requestKey, "engineKey", engineKey)
		return nil
	}
	remaining := merged[:0]
	removed := false
	for _, ref := range merged {
		match := false
		for _, record := range records {
			if ref == record {
				match = true
				removed = true
				break
			}
		}
		if !match {
			remaining = append(remaining, ref)
		}
	}
	if !removed {
		return nil
	}
	if len(remaining) != 0 {
		evicted, didEvict, err := m.data.addTracked(requestKey, remaining)
		if err != nil {
			return fmt.Errorf("materialize request key %s after eviction: %w", requestKey.String(), err)
		}
		if didEvict {
			m.removeBaseRequestKeyLocked(evicted)
		}
	}
	m.removeBaseRequestKeyLocked(requestKey)

	traceLogger.Info("evicted pods from key", "requestKey", requestKey, "engineKey", engineKey, "pods", entries)
	return nil
}

// Clear removes every entry for the pod from the index, across all device tiers.
// O(N) over the index, but Clear is rare and off the Lookup/Add hot path. It
// snapshots key generations, then locks and updates one key at a time.
//
// Context cancellation does not interrupt Clear.
//
// The engineKey->requestKey mapping (engineToRequestKeys) is intentionally left
// untouched: it is LRU-bounded, self-heals when the pod re-Adds the same prefixes,
// and any stale mapping resolves to an emptied request key that correctly breaks
// the prefix chain in Lookup.
func (m *InMemoryIndex) Clear(ctx context.Context, podIdentifier string) error {
	return m.clear(ctx, podIdentifier, nil)
}

func (m *InMemoryIndex) ClearRank(ctx context.Context, podIdentifier string, dataParallelRank int) error {
	return m.clear(ctx, podIdentifier, &dataParallelRank)
}

func (m *InMemoryIndex) clear(ctx context.Context, podIdentifier string, dataParallelRank *int) error {
	m.snapshotMu.RLock()
	defer m.snapshotMu.RUnlock()
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()
	m.baseStateMu.Lock()
	defer m.baseStateMu.Unlock()
	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.InMemoryIndex.Clear")

	m.pods.mu.Lock()
	pod, found := m.pods.ids[podIdentifier]
	m.pods.mu.Unlock()
	if found {
		m.data.clearPod(pod, dataParallelRank)
		m.clearBasePodLocked(pod, dataParallelRank)
	}

	traceLogger.Info("cleared pod from index", "pod", podIdentifier, "dataParallelRank", dataParallelRank)
	return nil
}

// GetRequestKey returns the last request key (highest index in the chain) associated with the given engineKey.
// This is what Pool uses for parent hash resolution.
// Returns an error if the engineKey mapping is missing (e.g., already evicted).
func (m *InMemoryIndex) GetRequestKey(ctx context.Context, engineKey BlockHash) (BlockHash, error) {
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	m.baseStateMu.RLock()
	defer m.baseStateMu.RUnlock()
	rks, found := m.requestKeysLocked(engineKey, nil)
	if !found || len(rks) == 0 {
		return EmptyBlockHash, fmt.Errorf("engine key not found: %s", engineKey.String())
	}
	return rks[len(rks)-1], nil
}

// podsPerKeyPrintHelper formats a map of keys to pod names for printing.
func podsPerKeyPrintHelper(ks map[BlockHash][]PodEntry) string {
	var b strings.Builder
	for k, v := range ks {
		fmt.Fprintf(&b, "%s: %v\n", k.String(), collections.SliceMap(v, func(pod PodEntry) string {
			return pod.String()
		}))
	}
	return b.String()
}
