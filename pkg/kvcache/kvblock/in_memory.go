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
	"sync/atomic"

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

	index := &InMemoryIndex{
		baseClears: make(map[uint32]baseClear),
	}
	index.view.Store(&indexView{
		data:                cache,
		engineToRequestKeys: engineToRequestKeys,
		pods:                pods,
		tiers:               tiers,
	})
	return index, nil
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

// indexView is the immutable field set a restore or install swaps in one
// atomic publish. Readers load it once per operation.
type indexView struct {
	// data holds the mapping of requestKeys to sets of pod identifiers.
	data *slabStore
	// engineToRequestKeys holds the mapping of engineKeys to requestKeys.
	engineToRequestKeys *lru.Cache[BlockHash, []BlockHash]
	// pods and tiers assign the ordinals compact records carry. Neither is
	// reclaimed when entries leave the index: pod identifiers are endpoint
	// address:port values, bounded by the pod network's address space, and
	// tiers are the engine-reported names. Each is capped, and an Add past
	// a cap fails rather than admitting an entry that cannot be matched.
	pods  *interner
	tiers *interner
	base  *mappedSnapshot
	// refs pins a view that carries a mapped base: it stays positive from a
	// reader's acquire until its release, so a concurrent install cannot
	// unmap the bytes mid-operation. Only the writer mutates retired, and a
	// reader that observes retired reloads the current view instead.
	refs    atomic.Int32
	retired atomic.Bool
}

func (v *indexView) release() { v.refs.Add(-1) }

// InMemoryIndex keeps new records in memory and can read a mapped snapshot base.
//
// Mutation contract: one goroutine performs every mutation (Add, Evict, Clear,
// checkpoint writes, restores). The event pool supplies that goroutine.
// Lookup and the walks serve any number of concurrent readers. The writer
// needs no mutation lock. baseStateMu serializes the writer against whole
// read operations, so a lookup runs without the writer interleaving slab
// mutations through its key loop; readers share it without blocking.
type InMemoryIndex struct {
	// view publishes the current field set. Swaps happen on restore and
	// install; a view with a mapped base is retired and closed once its
	// in-flight readers release it.
	view atomic.Pointer[indexView]
	// baseStateMu pairs every writer mutation with one complete read
	// operation at a time, and guards the base maps and baseClears while a
	// mapped base exists.
	baseStateMu sync.RWMutex
	// baseClears masks entries of cleared pods inside the immutable mapped
	// base. Guarded by baseStateMu; empty while no base is installed.
	baseClears map[uint32]baseClear
	// retiredViews are views whose replacement was published while readers
	// still held them. Writer-owned; drained after each mutation.
	retiredViews []*indexView
}

var _ Index = &InMemoryIndex{}
var _ CompactKeyWalker = &InMemoryIndex{}

// writerView returns the field set the single writer mutates. It never
// retires the view it uses.
func (m *InMemoryIndex) writerView() *indexView {
	return m.view.Load()
}

// acquireView pins the current view for one read operation and releases
// retired views instead of using them.
func (m *InMemoryIndex) acquireView() *indexView {
	for {
		v := m.view.Load()
		if v.base == nil {
			return v
		}
		v.refs.Add(1)
		if !v.retired.Load() {
			return v
		}
		v.refs.Add(-1)
	}
}

// publishView installs next on the writer goroutine and retires the view it
// replaces. A retired view's mapped base closes once its readers release it.
func (m *InMemoryIndex) publishView(next *indexView) {
	old := m.view.Swap(next)
	if old == nil || old.base == nil {
		return
	}
	old.retired.Store(true)
	if old.refs.Load() == 0 {
		old.base.close()
		return
	}
	m.retiredViews = append(m.retiredViews, old)
}

func (m *InMemoryIndex) drainRetiredViews() {
	remaining := m.retiredViews[:0]
	for _, v := range m.retiredViews {
		if v.refs.Load() == 0 {
			v.base.close()
			continue
		}
		remaining = append(remaining, v)
	}
	m.retiredViews = remaining
}

// PodCache is an opaque compatibility type. In-memory indexes manage per-key
// storage internally.
//
// Deprecated: Do not use this type in new code.
type PodCache struct{}

// internRecords pairs each entry with its pod and tier ordinals. A batch is
// assigned all or nothing: when its new pods or tiers would exceed a cap, no
// ordinal is consumed and the error names the cap.
func (m *InMemoryIndex) internRecords(v *indexView, entries []PodEntry) ([]slabRef, error) {
	v.pods.mu.Lock()
	defer v.pods.mu.Unlock()
	v.tiers.mu.Lock()
	defer v.tiers.mu.Unlock()

	if !v.pods.fitsLocked(func(yield func(string)) {
		for i := range entries {
			yield(entries[i].PodIdentifier)
		}
	}) {
		return nil, fmt.Errorf("%w: %d pod identifiers", errIndexCardinality, maxInternedPods)
	}
	if !v.tiers.fitsLocked(func(yield func(string)) {
		for i := range entries {
			yield(entries[i].DeviceTier)
		}
	}) {
		return nil, fmt.Errorf("%w: %d device tiers", errIndexCardinality, maxInternedTiers)
	}

	records := make([]slabRef, len(entries))
	for i, entry := range entries {
		records[i] = newSlabRef(entry,
			v.pods.internLocked(entry.PodIdentifier),
			v.tiers.internLocked(entry.DeviceTier))
	}
	return records, nil
}

// knownRecords encodes entries already known to the interners. Unknown names
// cannot match an indexed record and are omitted.
func (m *InMemoryIndex) knownRecords(v *indexView, entries []PodEntry, records []slabRef) []slabRef {
	v.pods.mu.Lock()
	defer v.pods.mu.Unlock()
	v.tiers.mu.Lock()
	defer v.tiers.mu.Unlock()

	for _, entry := range entries {
		pod, podFound := v.pods.ids[entry.PodIdentifier]
		tier, tierFound := v.tiers.ids[entry.DeviceTier]
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
	v := m.acquireView()
	defer v.release()
	// baseStateMu gives lookups priority over the writer: a lookup holds the
	// read lock for its whole duration, so the writer cannot interleave slab
	// mutations through the key loop.
	m.baseStateMu.RLock()
	defer m.baseStateMu.RUnlock()

	podsPerKey := make(map[BlockHash][]PodEntry)
	filtered := podIdentifierSet.Len() > 0
	var allowed map[uint32]struct{}
	if filtered {
		allowed = make(map[uint32]struct{}, podIdentifierSet.Len())
		v.pods.mu.Lock()
		for pod := range podIdentifierSet {
			if ordinal, found := v.pods.ids[pod]; found {
				allowed[ordinal] = struct{}{}
			}
		}
		v.pods.mu.Unlock()
	}
	highestHitIdx := 0
	visited := 0
	refs := make([]CompactEntryRef, 0, int(v.data.entryCap))
	// Every exit, cancellation included, refreshes what was read.
	defer func() { v.data.promote(requestKeys[:visited]) }()

	for idx, requestKey := range requestKeys {
		if idx&cancellationCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		refs, found := m.mergedCompactEntriesLocked(v, requestKey, refs)
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
			entries = append(entries, ref.entry(v.pods, v.tiers).PodEntry)
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
	v := m.writerView()
	m.baseStateMu.Lock()
	defer m.baseStateMu.Unlock()

	storeLocked := !v.data.hasWorstCaseAddCapacity(len(requestKeys))
	if storeLocked {
		v.data.mu.Lock()
		if err := v.data.ensureAddCapacityLocked(requestKeys, entries); err != nil {
			v.data.mu.Unlock()
			return fmt.Errorf("index cannot add request keys: %w", err)
		}
		v.data.mu.Unlock()
	}

	// Intern once per call, before anything is written: a rejected batch
	// leaves no mapping and no ordinal behind. The same records apply to
	// every request key.
	records, err := m.internRecords(v, entries)
	if err != nil {
		return err
	}

	for _, requestKey := range requestKeys {
		if err := m.addRequestRecordsLocked(v, requestKey, records); err != nil {
			return fmt.Errorf("failed to add request key: %w", err)
		}

		if traceLogger.Enabled() {
			traceLogger.Info("added pods to key", "requestKey", requestKey, "pods", entries)
		}
	}
	if engineKeys != nil {
		mappings := engineToRequestMapping(engineKeys, requestKeys)
		for ek, rks := range mappings {
			m.addEngineMappingLocked(v, ek, rks)
		}
	}
	m.drainRetiredViews()

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
	v := m.writerView()
	m.baseStateMu.Lock()
	defer m.baseStateMu.Unlock()
	var recordBuffer [1]slabRef // KV removal events contain one pod entry.
	records := recordBuffer[:0]
	if len(entries) > len(recordBuffer) {
		records = make([]slabRef, 0, len(entries))
	}
	records = m.knownRecords(v, entries, records)

	switch keyType {
	case EngineKey:
		rks, found := m.requestKeysLocked(v, key, nil)
		if !found {
			traceLogger.Info("engineKey not found in mapping, nothing to evict", "engineKey", key)
			return nil
		}

		for _, rk := range rks {
			if err := m.evictPodsFromRequestKey(v, rk, key, records, entries, traceLogger); err != nil {
				return err
			}
		}

		allEmpty := true
		if v.base == nil {
			for _, rk := range rks {
				if v.data.size(rk) > 0 {
					allEmpty = false
					break
				}
			}
		} else {
			refs := make([]CompactEntryRef, 0, int(v.data.entryCap))
			for _, rk := range rks {
				refs, _ = m.mergedCompactEntriesLocked(v, rk, refs[:0])
				if len(refs) > 0 {
					allEmpty = false
					break
				}
			}
		}
		if allEmpty {
			v.engineToRequestKeys.Remove(key)
			if v.base != nil {
				delete(v.base.engineOffsets, key)
			}
		}
		m.drainRetiredViews()
		return nil
	case RequestKey:
		err := m.evictPodsFromRequestKey(v, key, EmptyBlockHash, records, entries, traceLogger)
		m.drainRetiredViews()
		return err
	default:
		return fmt.Errorf("unknown key type: %d", keyType)
	}
}

// evictPodsFromRequestKey removes the given pod entries from a single request key's cache.
// If the cache becomes empty, the request key is removed from the index.
func (m *InMemoryIndex) evictPodsFromRequestKey(
	v *indexView, requestKey, engineKey BlockHash, records []slabRef,
	entries []PodEntry, traceLogger logr.Logger,
) error {
	baseFound := false
	if v.base != nil {
		_, baseFound = v.base.requestOffsets[requestKey]
	}
	if !baseFound {
		if !v.data.remove(requestKey, records) {
			traceLogger.Info("requestKey not found in index, nothing to evict", "requestKey", requestKey, "engineKey", engineKey)
			return nil
		}
		traceLogger.Info("evicted pods from key", "requestKey", requestKey, "engineKey", engineKey, "pods", entries)
		return nil
	}
	merged, found := m.mergedCompactEntriesLocked(v, requestKey, nil)
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
		evicted, didEvict, err := v.data.addTracked(requestKey, remaining)
		if err != nil {
			return fmt.Errorf("materialize request key %s after eviction: %w", requestKey.String(), err)
		}
		if didEvict {
			m.removeBaseRequestKeyLocked(v, evicted)
		}
	}
	m.removeBaseRequestKeyLocked(v, requestKey)

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
	traceLogger := log.FromContext(ctx).V(logging.TRACE).WithName("kvblock.InMemoryIndex.Clear")
	v := m.writerView()
	m.baseStateMu.Lock()
	defer m.baseStateMu.Unlock()

	v.pods.mu.Lock()
	pod, found := v.pods.ids[podIdentifier]
	v.pods.mu.Unlock()
	if found {
		v.data.clearPod(pod, dataParallelRank)
		m.clearBasePodLocked(v, pod, dataParallelRank)
	}
	m.drainRetiredViews()

	traceLogger.Info("cleared pod from index", "pod", podIdentifier, "dataParallelRank", dataParallelRank)
	return nil
}

// GetRequestKey returns the last request key (highest index in the chain) associated with the given engineKey.
// This is what Pool uses for parent hash resolution.
// Returns an error if the engineKey mapping is missing (e.g., already evicted).
func (m *InMemoryIndex) GetRequestKey(ctx context.Context, engineKey BlockHash) (BlockHash, error) {
	v := m.acquireView()
	defer v.release()
	// baseStateMu gives lookups priority over the writer: a lookup holds the
	// read lock for its whole duration, so the writer cannot interleave slab
	// mutations through the key loop.
	m.baseStateMu.RLock()
	defer m.baseStateMu.RUnlock()
	rks, found := m.requestKeysLocked(v, engineKey, nil)
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
