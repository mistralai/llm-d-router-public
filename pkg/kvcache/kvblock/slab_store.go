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
	"errors"
	"sync"
)

const (
	slabChunkBits = 16
	slabChunkSize = 1 << slabChunkBits
	slabChunkMask = slabChunkSize - 1
	maxRefChunks  = 1 << (32 - slabChunkBits)

	tierMask       = 1<<12 - 1
	speculativeBit = 1 << 12
	hasGroupBit    = 1 << 13
)

type slabRef = CompactEntryRef

type nodeID uint32

type runHead uint32

func newSlabRef(entry PodEntry, pod, tier uint32) slabRef {
	return newCompactEntryRef(pod, tier, entry.Speculative, entry.HasGroup, entry.GroupIdx)
}

func (r slabRef) entry(pods, tiers *interner) EntryRef {
	tier := r.tierAndFlags & tierMask
	return EntryRef{
		PodEntry: PodEntry{
			PodIdentifier: pods.name(r.PodOrdinal),
			DeviceTier:    tiers.name(tier),
			Speculative:   r.tierAndFlags&speculativeBit != 0,
			HasGroup:      r.tierAndFlags&hasGroupBit != 0,
			GroupIdx:      r.group,
		},
		PodOrdinal:  r.PodOrdinal,
		TierOrdinal: tier,
	}
}

type slabNode struct {
	hash    BlockHash
	head    runHead
	runLen  uint16
	runCap  uint16
	prev    nodeID
	next    nodeID
	version uint64
	mu      sync.Mutex
}

type runBump struct {
	chunk       uint32
	offset      uint32
	initialized bool
}

// slabStore keeps key metadata and compact entry records in stable chunks.
// Node and entry addresses remain valid while concurrent readers finish.
// Entry runs use fixed size classes. A run grows to the smallest class that
// fits and does not shrink. Freed runs remain in their class for reuse. Chunks
// remain allocated, so resident memory reflects peak demand in each class.
type slabStore struct {
	mu       sync.RWMutex
	refsMu   sync.Mutex
	capacity int
	entryCap uint16
	items    map[BlockHash]nodeID
	len      int
	head     nodeID
	tail     nodeID

	nodeChunks   []*[slabChunkSize]slabNode
	nextNode     uint64
	nextVersion  uint64
	freeNodeHead nodeID

	refChunks []*[slabChunkSize]slabRef
	nextChunk uint32
	runBumps  []runBump
	freeRuns  []runHead

	pods        *interner
	tiers       *interner
	entryBuffer sync.Pool
}

func newSlabStore(size, entryCap int, pods, tiers *interner) (*slabStore, error) {
	if size <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if uint64(size) > uint64(^uint32(0)) {
		return nil, errors.New("size exceeds slab node capacity")
	}
	if entryCap <= 0 || entryCap > int(^uint16(0)) {
		return nil, errors.New("pod cache size must be between 1 and 65535")
	}
	nodeChunkCount := (uint64(size) + 1 + slabChunkMask) >> slabChunkBits
	s := &slabStore{
		capacity:   size,
		entryCap:   uint16(entryCap),
		items:      make(map[BlockHash]nodeID),
		nodeChunks: make([]*[slabChunkSize]slabNode, int(nodeChunkCount)),
		nextNode:   1,
		refChunks:  make([]*[slabChunkSize]slabRef, maxRefChunks),
		runBumps:   make([]runBump, entryCap+1),
		freeRuns:   make([]runHead, entryCap+1),
		pods:       pods,
		tiers:      tiers,
	}
	s.entryBuffer.New = func() any {
		entries := make([]EntryRef, 0, entryCap)
		return &entries
	}
	return s, nil
}

func runCapacities(limit int) []uint16 {
	capacities := make([]uint16, 0, 17)
	for capacity := uint32(1); capacity < uint32(limit); capacity *= 2 {
		capacities = append(capacities, uint16(capacity))
	}
	if len(capacities) == 0 || capacities[len(capacities)-1] != uint16(limit) {
		capacities = append(capacities, uint16(limit))
	}
	return capacities
}

func (s *slabStore) node(id nodeID) *slabNode {
	raw := uint32(id)
	return &s.nodeChunks[raw>>slabChunkBits][raw&slabChunkMask]
}

func (s *slabStore) refs(head runHead, capacity uint16) []slabRef {
	if capacity == 0 {
		return nil
	}
	raw := uint32(head)
	chunk := s.refChunks[raw>>slabChunkBits]
	offset := raw & slabChunkMask
	return chunk[offset : offset+uint32(capacity)]
}

func (s *slabStore) allocNodeLocked(hash BlockHash) (nodeID, *slabNode, error) {
	var id nodeID
	if s.freeNodeHead != 0 {
		id = s.freeNodeHead
		n := s.node(id)
		s.freeNodeHead = n.next
	} else {
		if s.nextNode > uint64(^uint32(0)) {
			return 0, nil, errors.New("slab node capacity exhausted")
		}
		id = nodeID(s.nextNode) // #nosec G115 -- bounds checked above.
		s.nextNode++
		chunkIdx := uint32(id) >> slabChunkBits
		if s.nodeChunks[chunkIdx] == nil {
			s.nodeChunks[chunkIdx] = new([slabChunkSize]slabNode)
		}
	}

	n := s.node(id)
	n.mu.Lock()
	s.nextVersion++
	n.hash = hash
	n.head = 0
	n.runLen = 0
	n.runCap = 0
	n.prev = 0
	n.next = 0
	n.version = s.nextVersion
	n.mu.Unlock()
	return id, n, nil
}

func (s *slabStore) allocRun(capacity uint16) (runHead, error) {
	s.refsMu.Lock()
	defer s.refsMu.Unlock()
	return s.allocRunLocked(capacity)
}

func (s *slabStore) allocRunLocked(capacity uint16) (runHead, error) {
	if head := s.freeRuns[capacity]; head != 0 {
		s.freeRuns[capacity] = runHead(s.refs(head, capacity)[0].PodOrdinal)
		return head, nil
	}

	bump := &s.runBumps[capacity]
	if !bump.initialized || bump.offset+uint32(capacity) > slabChunkSize {
		if int(s.nextChunk) >= len(s.refChunks) {
			return 0, errors.New("slab reference capacity exhausted")
		}
		bump.chunk = s.nextChunk
		bump.offset = 0
		bump.initialized = true
		s.refChunks[s.nextChunk] = new([slabChunkSize]slabRef)
		s.nextChunk++
		if bump.chunk == 0 {
			bump.offset = 1
		}
	}
	head := runHead(bump.chunk<<slabChunkBits | bump.offset)
	bump.offset += uint32(capacity)
	return head, nil
}

func (s *slabStore) freeRun(head runHead, capacity uint16) {
	if capacity == 0 {
		return
	}
	s.refsMu.Lock()
	defer s.refsMu.Unlock()
	refs := s.refs(head, capacity)
	refs[0].PodOrdinal = uint32(s.freeRuns[capacity])
	s.freeRuns[capacity] = head
}

// Add calls are serialized. Concurrent operations only return runs, so this
// estimate does not require allocator locks.
func (s *slabStore) hasWorstCaseAddCapacity(keys int) bool {
	if keys <= 0 {
		return true
	}

	classes := 0
	for capacity := uint32(1); ; {
		classes++
		if capacity >= uint32(s.entryCap) {
			break
		}
		capacity = min(capacity*2, uint32(s.entryCap))
	}
	remainingChunks := uint64(len(s.refChunks)) - uint64(s.nextChunk)
	keyCount := uint64(keys)
	return keyCount <= remainingChunks/uint64(classes)
}

// ensureAddCapacityLocked checks a conservative capacity requirement. The
// caller holds s.mu during this check and all batch additions.
func (s *slabStore) ensureAddCapacityLocked(keys []BlockHash, entries []PodEntry) error {
	neededRuns := make([]uint64, len(s.runBumps))
	incomingLen := 0
	for i, entry := range entries {
		duplicate := false
		for j := 0; j < i; j++ {
			if entries[j] == entry {
				duplicate = true
				break
			}
		}
		if !duplicate && incomingLen < int(s.entryCap) {
			incomingLen++
		}
	}
	incomingCap := s.runCapacity(incomingLen)
	allPresentWithoutGrowth := true
	seenKeys := make(map[BlockHash]struct{}, len(keys))
	newKeys := 0
	for _, key := range keys {
		if _, seen := seenKeys[key]; seen {
			continue
		}
		seenKeys[key] = struct{}{}
		if _, present := s.items[key]; !present {
			newKeys++
		}
	}
	batchKeyCanBeEvicted := s.len+newKeys > s.capacity
	clear(seenKeys)
	for _, key := range keys {
		if _, seen := seenKeys[key]; seen {
			continue
		}
		seenKeys[key] = struct{}{}

		currentLen := 0
		currentCap := uint16(0)
		var refs []slabRef
		var n *slabNode
		_, present := s.items[key]
		if present {
			id := s.items[key]
			n = s.node(id)
			n.mu.Lock()
			currentLen = int(n.runLen)
			currentCap = n.runCap
			refs = s.refs(n.head, n.runCap)[:n.runLen]
		}

		required := currentLen
		for i, entry := range entries {
			found := false
			for _, ref := range refs {
				if ref.entry(s.pods, s.tiers).PodEntry == entry {
					found = true
					break
				}
			}
			if !found {
				for j := 0; j < i; j++ {
					if entries[j] == entry {
						found = true
						break
					}
				}
			}
			if !found && required < int(s.entryCap) {
				required++
			}
		}
		if n != nil {
			n.mu.Unlock()
		}

		if currentCap < uint16(required) {
			allPresentWithoutGrowth = false
			growthCap := s.runCapacity(required)
			if growthCap != incomingCap {
				neededRuns[growthCap]++
			}
		}
		if !present {
			allPresentWithoutGrowth = false
		}
		if !present || batchKeyCanBeEvicted {
			neededRuns[incomingCap]++
		}
	}
	if allPresentWithoutGrowth {
		return nil
	}

	s.refsMu.Lock()
	defer s.refsMu.Unlock()

	remainingChunks := uint64(len(s.refChunks)) - uint64(s.nextChunk)
	requiredChunks := uint64(0)
	for capacity := uint32(1); ; {
		needed := neededRuns[capacity]
		for head := s.freeRuns[capacity]; head != 0 && needed > 0; needed-- {
			head = runHead(s.refs(head, uint16(capacity))[0].PodOrdinal)
		}
		bump := s.runBumps[capacity]
		if needed > 0 && bump.initialized {
			available := uint64((slabChunkSize - bump.offset) / capacity)
			if available >= needed {
				needed = 0
			} else {
				needed -= available
			}
		}
		if needed > 0 {
			runsPerChunk := uint64(slabChunkSize / capacity)
			chunks := (needed + runsPerChunk - 1) / runsPerChunk
			if chunks > remainingChunks-requiredChunks {
				return errors.New("slab reference capacity exhausted")
			}
			requiredChunks += chunks
		}
		if requiredChunks > remainingChunks {
			return errors.New("slab reference capacity exhausted")
		}
		if capacity >= uint32(s.entryCap) {
			break
		}
		capacity = min(capacity*2, uint32(s.entryCap))
	}
	// The first allocation can lose one run because address zero is the
	// free-list sentinel. One additional chunk covers that loss regardless of
	// which size class is allocated first.
	if s.nextChunk == 0 && requiredChunks > 0 {
		if requiredChunks == remainingChunks {
			return errors.New("slab reference capacity exhausted")
		}
	}
	return nil
}

func (s *slabStore) growRun(n *slabNode, newCap uint16) error {
	newHead, err := s.allocRun(newCap)
	if err != nil {
		return err
	}
	newRefs := s.refs(newHead, newCap)
	oldHead, oldCap := n.head, n.runCap
	if n.runLen > 0 {
		copy(newRefs, s.refs(oldHead, oldCap)[:n.runLen])
	}
	if oldCap > 0 {
		s.freeRun(oldHead, oldCap)
	}
	n.head = newHead
	n.runCap = newCap
	return nil
}

func (s *slabStore) runCapacity(required int) uint16 {
	capacity := uint32(1)
	for capacity < uint32(required) {
		capacity = min(capacity*2, uint32(s.entryCap))
	}
	return uint16(capacity)
}

func (s *slabStore) addAllLocked(n *slabNode, records []slabRef) error {
	required := int(n.runLen)
	refs := s.refs(n.head, n.runCap)
	for i, record := range records {
		found := false
		for j := 0; j < int(n.runLen); j++ {
			if refs[j] == record {
				found = true
				break
			}
		}
		if !found {
			for j := 0; j < i; j++ {
				if records[j] == record {
					found = true
					break
				}
			}
		}
		if !found && required < int(s.entryCap) {
			required++
		}
	}
	if int(n.runCap) < required {
		if err := s.growRun(n, s.runCapacity(required)); err != nil {
			return err
		}
	}

	for _, record := range records {
		refs = s.refs(n.head, n.runCap)
		found := -1
		for i := 0; i < int(n.runLen); i++ {
			if refs[i] == record {
				found = i
				break
			}
		}
		if found >= 0 {
			copy(refs[found:], refs[found+1:n.runLen])
			refs[n.runLen-1] = record
			continue
		}
		if n.runLen == s.entryCap {
			copy(refs, refs[1:n.runLen])
			refs[n.runLen-1] = record
			continue
		}
		refs[n.runLen] = record
		n.runLen++
	}
	return nil
}

func (s *slabStore) add(key BlockHash, records []slabRef) error {
	for {
		s.mu.Lock()
		if id, found := s.items[key]; found {
			n := s.node(id)
			version := n.version
			s.moveToHeadLocked(id, n)
			s.mu.Unlock()

			n.mu.Lock()
			if n.hash != key || n.version != version {
				n.mu.Unlock()
				continue
			}
			err := s.addAllLocked(n, records)
			n.mu.Unlock()
			return err
		}

		err := s.addNewWithStoreLockHeld(key, records)
		s.mu.Unlock()
		return err
	}
}

func (s *slabStore) addWithStoreLockHeld(key BlockHash, records []slabRef) error {
	if id, found := s.items[key]; found {
		n := s.node(id)
		s.moveToHeadLocked(id, n)
		n.mu.Lock()
		err := s.addAllLocked(n, records)
		n.mu.Unlock()
		return err
	}
	return s.addNewWithStoreLockHeld(key, records)
}

func (s *slabStore) addNewWithStoreLockHeld(key BlockHash, records []slabRef) error {
	var staged slabNode
	if err := s.addAllLocked(&staged, records); err != nil {
		s.freeRun(staged.head, staged.runCap)
		return err
	}
	if s.len == s.capacity {
		s.releaseNodeLocked(s.tail)
	}
	id, n, err := s.allocNodeLocked(key)
	if err != nil {
		s.freeRun(staged.head, staged.runCap)
		return err
	}
	n.mu.Lock()
	n.head = staged.head
	n.runLen = staged.runLen
	n.runCap = staged.runCap
	n.mu.Unlock()
	s.items[key] = id
	s.insertHeadLocked(id, n)
	s.len++
	return nil
}

func (s *slabStore) capture(key BlockHash, promote bool) (*slabNode, uint64, bool) {
	if promote {
		s.mu.Lock()
		defer s.mu.Unlock()
		id, found := s.items[key]
		if !found {
			return nil, 0, false
		}
		n := s.node(id)
		s.moveToHeadLocked(id, n)
		return n, n.version, true
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	id, found := s.items[key]
	if !found {
		return nil, 0, false
	}
	n := s.node(id)
	return n, n.version, true
}

func (s *slabStore) filteredEntries(key BlockHash, allowed map[uint32]struct{}, filtered bool) ([]PodEntry, int, bool) {
	n, version, found := s.capture(key, false)
	if !found {
		return nil, 0, false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hash != key || n.version != version {
		return nil, 0, false
	}

	total := int(n.runLen)
	var entries []PodEntry
	if !filtered && total > 0 {
		entries = make([]PodEntry, 0, total)
	}
	refs := s.refs(n.head, n.runCap)
	for i := 0; i < total; i++ {
		if filtered {
			if _, ok := allowed[refs[i].PodOrdinal]; !ok {
				continue
			}
		}
		entries = append(entries, refs[i].entry(s.pods, s.tiers).PodEntry)
	}
	return entries, total, true
}

func (s *slabStore) visit(key BlockHash, pos int, entries *[]EntryRef,
	visit func(int, bool, []EntryRef) bool,
) (found, keepGoing bool) {
	n, version, found := s.capture(key, false)
	if !found {
		return false, visit(pos, false, nil)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hash != key || n.version != version {
		return false, visit(pos, false, nil)
	}

	refs := s.refs(n.head, n.runCap)
	*entries = (*entries)[:0]
	for i := 0; i < int(n.runLen); i++ {
		*entries = append(*entries, refs[i].entry(s.pods, s.tiers))
	}
	return true, visit(pos, true, *entries)
}

func (s *slabStore) visitCompact(key BlockHash, pos int,
	visit func(int, bool, []CompactEntryRef) bool,
) (found, keepGoing bool) {
	n, version, found := s.capture(key, false)
	if !found {
		return false, visit(pos, false, nil)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hash != key || n.version != version {
		return false, visit(pos, false, nil)
	}
	return true, visit(pos, true, s.refs(n.head, n.runCap)[:n.runLen])
}

func (s *slabStore) borrowEntries() *[]EntryRef {
	return s.entryBuffer.Get().(*[]EntryRef)
}

func (s *slabStore) returnEntries(entries *[]EntryRef) {
	clear(*entries)
	*entries = (*entries)[:0]
	s.entryBuffer.Put(entries)
}

func (s *slabStore) promote(keys []BlockHash) {
	if len(keys) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		if id, found := s.items[key]; found {
			s.moveToHeadLocked(id, s.node(id))
		}
	}
}

func (s *slabStore) size(key BlockHash) int {
	for {
		n, version, found := s.capture(key, true)
		if !found {
			return 0
		}
		n.mu.Lock()
		if n.hash == key && n.version == version {
			size := int(n.runLen)
			n.mu.Unlock()
			return size
		}
		n.mu.Unlock()
	}
}

func (s *slabStore) remove(key BlockHash, records []slabRef) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, found := s.items[key]
	if !found {
		return false
	}
	n := s.node(id)
	s.moveToHeadLocked(id, n)
	n.mu.Lock()
	defer n.mu.Unlock()

	refs := s.refs(n.head, n.runCap)
	for _, record := range records {
		for i := 0; i < int(n.runLen); i++ {
			if refs[i] != record {
				continue
			}
			copy(refs[i:], refs[i+1:n.runLen])
			n.runLen--
			refs[n.runLen] = slabRef{}
			break
		}
	}
	if n.runLen == 0 {
		s.releaseNodeWithLockHeld(id, n)
	}
	return true
}

func (s *slabStore) clearPod(pod uint32) {
	s.mu.RLock()
	nextNode := s.nextNode
	maxVersion := s.nextVersion
	s.mu.RUnlock()

	for id64 := uint64(1); id64 < nextNode; id64++ {
		id := nodeID(id64) // #nosec G115 -- nextNode never exceeds the uint32 address space.
		n := s.node(id)
		n.mu.Lock()
		if n.runCap == 0 || n.version > maxVersion {
			n.mu.Unlock()
			continue
		}

		refs := s.refs(n.head, n.runCap)
		write := 0
		for read := 0; read < int(n.runLen); read++ {
			if refs[read].PodOrdinal == pod {
				continue
			}
			refs[write] = refs[read]
			write++
		}
		clear(refs[write:n.runLen])
		n.runLen = uint16(write)
		empty := n.runLen == 0
		n.mu.Unlock()

		if !empty {
			continue
		}
		s.mu.Lock()
		n.mu.Lock()
		if current, found := s.items[n.hash]; found && current == id && n.runLen == 0 && n.version <= maxVersion {
			s.releaseNodeWithLockHeld(id, n)
		}
		n.mu.Unlock()
		s.mu.Unlock()
	}
}

func (s *slabStore) releaseNodeLocked(id nodeID) {
	n := s.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	s.releaseNodeWithLockHeld(id, n)
}

func (s *slabStore) releaseNodeWithLockHeld(id nodeID, n *slabNode) {
	delete(s.items, n.hash)
	s.unlinkLocked(n)
	s.len--
	s.freeRun(n.head, n.runCap)
	n.hash = 0
	n.head = 0
	n.runLen = 0
	n.runCap = 0
	n.prev = 0
	n.next = s.freeNodeHead
	s.freeNodeHead = id
}

func (s *slabStore) insertHeadLocked(id nodeID, n *slabNode) {
	n.prev = 0
	n.next = s.head
	if s.head != 0 {
		s.node(s.head).prev = id
	} else {
		s.tail = id
	}
	s.head = id
}

func (s *slabStore) moveToHeadLocked(id nodeID, n *slabNode) {
	if s.head == id {
		return
	}
	s.unlinkLocked(n)
	s.insertHeadLocked(id, n)
}

func (s *slabStore) unlinkLocked(n *slabNode) {
	if n.prev != 0 {
		s.node(n.prev).next = n.next
	} else {
		s.head = n.next
	}
	if n.next != 0 {
		s.node(n.next).prev = n.prev
	} else {
		s.tail = n.prev
	}
}

// peek exposes a stable node pointer for package-internal lock tests.
func (s *slabStore) peek(key BlockHash) (*slabNode, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, found := s.items[key]
	if !found {
		return nil, false
	}
	return s.node(id), true
}
