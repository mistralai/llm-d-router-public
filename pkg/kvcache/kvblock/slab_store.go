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
	head    uint32
	runLen  uint16
	runCap  uint16
	prev    uint32
	next    uint32
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
type slabStore struct {
	mu       sync.RWMutex
	refsMu   sync.Mutex
	capacity int
	entryCap uint16
	items    map[BlockHash]uint32
	len      int
	head     uint32
	tail     uint32

	nodeChunks  []*[slabChunkSize]slabNode
	nextNode    uint64
	nextVersion uint64
	freeNodes   uint32

	refChunks []*[slabChunkSize]slabRef
	nextChunk uint32
	runBumps  []runBump
	freeRuns  []uint32

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
		items:      make(map[BlockHash]uint32),
		nodeChunks: make([]*[slabChunkSize]slabNode, int(nodeChunkCount)),
		nextNode:   1,
		refChunks:  make([]*[slabChunkSize]slabRef, maxRefChunks),
		runBumps:   make([]runBump, entryCap+1),
		freeRuns:   make([]uint32, entryCap+1),
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

func (s *slabStore) node(id uint32) *slabNode {
	return &s.nodeChunks[id>>slabChunkBits][id&slabChunkMask]
}

func (s *slabStore) refs(head uint32, capacity uint16) []slabRef {
	if capacity == 0 {
		return nil
	}
	chunk := s.refChunks[head>>slabChunkBits]
	offset := head & slabChunkMask
	return chunk[offset : offset+uint32(capacity)]
}

func (s *slabStore) allocNodeLocked(hash BlockHash) (uint32, *slabNode) {
	var id uint32
	if s.freeNodes != 0 {
		id = s.freeNodes
		n := s.node(id)
		s.freeNodes = n.head
	} else {
		id = uint32(s.nextNode)
		s.nextNode++
		chunkIdx := id >> slabChunkBits
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
	return id, n
}

func (s *slabStore) allocRun(capacity uint16) (uint32, error) {
	s.refsMu.Lock()
	defer s.refsMu.Unlock()

	if head := s.freeRuns[capacity]; head != 0 {
		s.freeRuns[capacity] = s.refs(head, capacity)[0].PodOrdinal
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
	head := bump.chunk<<slabChunkBits | bump.offset
	bump.offset += uint32(capacity)
	return head, nil
}

func (s *slabStore) freeRun(head uint32, capacity uint16) {
	if capacity == 0 {
		return
	}
	s.refsMu.Lock()
	defer s.refsMu.Unlock()
	refs := s.refs(head, capacity)
	refs[0].PodOrdinal = s.freeRuns[capacity]
	s.freeRuns[capacity] = head
}

func (s *slabStore) growRun(n *slabNode) error {
	newCap := uint32(1)
	if n.runCap > 0 {
		newCap = uint32(n.runCap) * 2
	}
	if newCap > uint32(s.entryCap) {
		newCap = uint32(s.entryCap)
	}

	newHead, err := s.allocRun(uint16(newCap))
	if err != nil {
		return err
	}
	newRefs := s.refs(newHead, uint16(newCap))
	if n.runLen > 0 {
		oldHead, oldCap := n.head, n.runCap
		copy(newRefs, s.refs(oldHead, oldCap)[:n.runLen])
		s.freeRun(oldHead, oldCap)
	}
	n.head = newHead
	n.runCap = uint16(newCap)
	return nil
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
	for int(n.runCap) < required {
		if err := s.growRun(n); err != nil {
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

		if s.len == s.capacity {
			s.releaseNodeLocked(s.tail)
		}
		id, n := s.allocNodeLocked(key)
		n.mu.Lock()
		err := s.addAllLocked(n, records)
		if err != nil {
			s.freeRun(n.head, n.runCap)
			n.hash = 0
			n.runLen = 0
			n.runCap = 0
			n.head = s.freeNodes
			s.freeNodes = id
			n.mu.Unlock()
			s.mu.Unlock()
			return err
		}
		n.mu.Unlock()
		s.items[key] = id
		s.insertHeadLocked(id, n)
		s.len++
		s.mu.Unlock()
		return nil
	}
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
		id := uint32(id64)
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

func (s *slabStore) releaseNodeLocked(id uint32) {
	n := s.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	s.releaseNodeWithLockHeld(id, n)
}

func (s *slabStore) releaseNodeWithLockHeld(id uint32, n *slabNode) {
	delete(s.items, n.hash)
	s.unlinkLocked(n)
	s.len--
	s.freeRun(n.head, n.runCap)
	n.hash = 0
	n.runLen = 0
	n.runCap = 0
	n.prev = 0
	n.next = 0
	n.head = s.freeNodes
	s.freeNodes = id
}

func (s *slabStore) insertHeadLocked(id uint32, n *slabNode) {
	n.prev = 0
	n.next = s.head
	if s.head != 0 {
		s.node(s.head).prev = id
	} else {
		s.tail = id
	}
	s.head = id
}

func (s *slabStore) moveToHeadLocked(id uint32, n *slabNode) {
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
