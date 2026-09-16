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
	"sync"
)

// EntryRef is one indexed entry with the ordinals the index assigned to its
// pod identifier and device tier. Two entries share a PodOrdinal exactly
// when they share a PodIdentifier, and a TierOrdinal exactly when they share
// a DeviceTier. An ordinal is assigned on the first Add of its name and
// belongs to that name for the index lifetime: Evict and Clear never reclaim
// or reassign one, and an Add whose new names would exceed the index's caps
// fails before writing anything. Live entries hold a sparse subset of the
// assigned range, so consumers key request-local tables by ordinals and
// never size state by them.
type EntryRef struct {
	PodEntry
	PodOrdinal  uint32
	TierOrdinal uint32
}

// CompactEntryRef is the pointer-free form used by compact index walkers.
// Names are resolved through CompactKeyWalker only when a consumer needs
// them.
type CompactEntryRef struct {
	PodOrdinal   uint32
	tierAndFlags uint32
	group        GroupID
}

// NewCompactEntryRef constructs a compact record for a CompactKeyWalker.
func NewCompactEntryRef(podOrdinal, tierOrdinal uint32, speculative, hasGroup bool,
	group GroupID,
) (CompactEntryRef, error) {
	if tierOrdinal > tierMask {
		return CompactEntryRef{}, fmt.Errorf("tier ordinal must be at most %d", tierMask)
	}
	return newCompactEntryRef(podOrdinal, tierOrdinal, speculative, hasGroup, group), nil
}

func newCompactEntryRef(podOrdinal, tierOrdinal uint32, speculative, hasGroup bool, group GroupID) CompactEntryRef {
	tierAndFlags := tierOrdinal
	if speculative {
		tierAndFlags |= speculativeBit
	}
	if hasGroup {
		tierAndFlags |= hasGroupBit
	}
	return CompactEntryRef{PodOrdinal: podOrdinal, tierAndFlags: tierAndFlags, group: group}
}

// TierOrdinal returns the interned device-tier ordinal.
func (r CompactEntryRef) TierOrdinal() uint32 { return r.tierAndFlags & tierMask }

// Speculative reports whether the entry is predictive.
func (r CompactEntryRef) Speculative() bool { return r.tierAndFlags&speculativeBit != 0 }

// HasGroup reports whether GroupIdx identifies a cache group.
func (r CompactEntryRef) HasGroup() bool { return r.tierAndFlags&hasGroupBit != 0 }

// GroupIdx returns the cache group identifier.
func (r CompactEntryRef) GroupIdx() GroupID { return r.group }

// KeyWalker is an optional Index capability: visit requestKeys in input
// order without materializing per-key entry slices.
//
// The walk contract:
//
//   - visit runs once per position of requestKeys, in order, until it
//     returns false, the keys run out, or ctx is cancelled. A key listed
//     twice is visited at both positions, each visit reading the index anew.
//   - found reports whether the index held a generation of the key when the
//     position was looked up. A miss is visited with found=false and no
//     entries; the walk continues.
//   - entries is every unfiltered record in that generation, in an order the
//     consumer must not rely on. It is borrowed: read-only and valid only
//     until visit returns, after which the index may reorder or overwrite its
//     backing array. Consumers copy what they keep.
//   - visit must not call back into the index; the generation's lock is held
//     for the duration of the call. If visit panics, the lock is released
//     and the panic propagates to the caller.
//   - A visit sees an internally consistent generation, exclusive of writers
//     and other visits using that generation. Capacity eviction may detach it
//     and a later Add may install a new generation of the same key while
//     visit runs. There is no snapshot across positions: a concurrent Add,
//     Evict, or Clear may be visible at some positions and not at others.
//   - Cancellation is polled at the first position, every 256th position
//     after it, and once more when the walk ends, so a cancelled ctx always
//     yields ctx.Err(); visits may run between the cancellation and the
//     next poll.
//   - When the walk ends, for any reason, the prefix of requestKeys through
//     the last key found is marked most recently used in position order
//     under one acquisition, skipping keys absent by then. Positions after
//     the last key found are not promoted. Lookup promotes the same way, so
//     a prefix that is only read stays resident under capacity pressure.
type KeyWalker interface {
	WalkKeys(ctx context.Context, requestKeys []BlockHash,
		visit func(pos int, found bool, entries []EntryRef) bool) error
}

// CompactKeyWalker avoids reconstructing strings for consumers that operate
// on stable pod and tier ordinals. Its walk follows the KeyWalker contract,
// except the callback may call PodName and TierName for ordinals it receives.
type CompactKeyWalker interface {
	KeyWalker
	WalkCompactKeys(ctx context.Context, requestKeys []BlockHash,
		visit func(pos int, found bool, entries []CompactEntryRef) bool) error
	PodName(ordinal uint32) string
	TierName(ordinal uint32) string
}

// WalkKeys implements KeyWalker. Each key is peeked under the LRU's shared
// lock and its entries visited under that key's own lock; the visited prefix
// is promoted in a deferred call, so every exit path refreshes what was read.
func (m *InMemoryIndex) WalkKeys(ctx context.Context, requestKeys []BlockHash,
	visit func(pos int, found bool, entries []EntryRef) bool,
) error {
	visited := 0
	entries := m.data.borrowEntries()
	defer m.data.returnEntries(entries)
	// Every exit, cancellation included, refreshes what was read.
	defer func() { m.data.promote(requestKeys[:visited]) }()
	for pos, key := range requestKeys {
		if pos&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		found, keepGoing := m.data.visit(key, pos, entries, visit)
		if found {
			visited = pos + 1
		}
		if !keepGoing {
			return ctx.Err()
		}
	}
	return ctx.Err()
}

// WalkCompactKeys implements CompactKeyWalker without materializing strings.
func (m *InMemoryIndex) WalkCompactKeys(ctx context.Context, requestKeys []BlockHash,
	visit func(pos int, found bool, entries []CompactEntryRef) bool,
) error {
	visited := 0
	defer func() { m.data.promote(requestKeys[:visited]) }()
	for pos, key := range requestKeys {
		if pos&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		found, keepGoing := m.data.visitCompact(key, pos, visit)
		if found {
			visited = pos + 1
		}
		if !keepGoing {
			return ctx.Err()
		}
	}
	return ctx.Err()
}

// PodName resolves an ordinal produced by this index.
func (m *InMemoryIndex) PodName(ordinal uint32) string { return m.pods.name(ordinal) }

// TierName resolves an ordinal produced by this index.
func (m *InMemoryIndex) TierName(ordinal uint32) string { return m.tiers.name(ordinal) }

// interner assigns dense uint32 ordinals to strings, stable for its lifetime
// and never reused, up to a fixed number of distinct strings. Callers hold mu
// across a whole batch so a batch is assigned all or nothing.
type interner struct {
	mu    sync.Mutex
	ids   map[string]uint32
	names []*[1024]string
	limit int
}

func newInterner(limit int) *interner {
	return &interner{
		ids:   make(map[string]uint32),
		names: make([]*[1024]string, (limit+1023)/1024),
		limit: limit,
	}
}

// fitsLocked reports whether the names not yet interned fit under the
// limit. Called with mu held.
func (in *interner) fitsLocked(names func(yield func(string))) bool {
	pending := 0
	seen := map[string]struct{}{}
	names(func(s string) {
		if _, ok := in.ids[s]; ok {
			return
		}
		if _, dup := seen[s]; dup {
			return
		}
		seen[s] = struct{}{}
		pending++
	})
	return len(in.ids)+pending <= in.limit
}

// internLocked returns the ordinal for s, assigning the next free one on
// first use. Called with mu held, after fitsLocked.
func (in *interner) internLocked(s string) uint32 {
	if id, ok := in.ids[s]; ok {
		return id
	}
	id := uint32(len(in.ids))
	in.ids[s] = id
	chunk := id / 1024
	if in.names[chunk] == nil {
		in.names[chunk] = new([1024]string)
	}
	in.names[chunk][id%1024] = s
	return id
}

// name returns the string assigned to id. Callers synchronize through the
// key generation that published the ordinal.
func (in *interner) name(id uint32) string {
	return in.names[id/1024][id%1024]
}
