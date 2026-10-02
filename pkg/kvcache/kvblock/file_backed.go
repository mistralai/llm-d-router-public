// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvblock

import "os"

type baseClear struct {
	all   bool
	ranks map[int]struct{}
}

type preparedMappedSnapshot struct {
	index       *InMemoryIndex
	base        *mappedSnapshot
	replacement *InMemoryIndex
	installed   bool
}

func (m *InMemoryIndex) PrepareSnapshotFile(
	file *os.File, offset, length int64,
) (PreparedFileSnapshot, error) {
	m.viewMu.RLock()
	capacity, entryCap := m.data.capacity, int(m.data.entryCap)
	m.viewMu.RUnlock()
	base, err := openMappedSnapshot(file, offset, length, capacity, entryCap)
	if err != nil {
		return nil, err
	}
	replacement, err := NewInMemoryIndex(&InMemoryIndexConfig{
		Size:         capacity,
		PodCacheSize: entryCap,
	})
	if err != nil {
		base.close()
		return nil, err
	}
	restoreInterner(replacement.pods, base.pods)
	restoreInterner(replacement.tiers, base.tiers)
	return &preparedMappedSnapshot{index: m, base: base, replacement: replacement}, nil
}

func (p *preparedMappedSnapshot) Install() error {
	if p.base == nil || p.installed {
		return os.ErrInvalid
	}
	m := p.index
	m.snapshotMu.Lock()
	m.viewMu.Lock()
	oldBase := m.base
	m.data = p.replacement.data
	m.engineToRequestKeys = p.replacement.engineToRequestKeys
	m.pods = p.replacement.pods
	m.tiers = p.replacement.tiers
	m.base = p.base
	m.baseClears = make(map[uint32]baseClear)
	m.viewMu.Unlock()
	m.snapshotMu.Unlock()
	oldBase.close()
	p.installed = true
	p.base = nil
	return nil
}

func (p *preparedMappedSnapshot) Close() {
	if p.base != nil {
		p.base.close()
		p.base = nil
	}
}

func (m *InMemoryIndex) RestoreSnapshotFile(file *os.File, offset, length int64) error {
	prepared, err := m.PrepareSnapshotFile(file, offset, length)
	if err != nil {
		return err
	}
	defer prepared.Close()
	return prepared.Install()
}

// mergedCompactEntriesLocked reads one generation. A first mutation moves the
// complete key from the mapped base to the overlay.
func (m *InMemoryIndex) mergedCompactEntriesLocked(key BlockHash, dst []CompactEntryRef) ([]CompactEntryRef, bool) {
	dst = dst[:0]
	if m.base != nil {
		baseEntries, found := m.base.compactEntries(key, dst)
		if found {
			dst = baseEntries
			write := 0
			for _, ref := range dst {
				if m.baseRefHiddenLocked(ref) {
					continue
				}
				dst[write] = ref
				write++
			}
			dst = dst[:write]
			return dst, len(dst) > 0
		}
	}
	return m.overlayCompactEntries(key, dst)
}

func (m *InMemoryIndex) overlayCompactEntries(key BlockHash, dst []CompactEntryRef) ([]CompactEntryRef, bool) {
	node, version, found := m.data.capture(key, false)
	if !found {
		return dst, false
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.hash != key || node.version != version {
		return dst, false
	}
	return append(dst, m.data.refs(node.head, node.runCap)[:node.runLen]...), true
}

func (m *InMemoryIndex) baseRefHiddenLocked(ref CompactEntryRef) bool {
	clearState, found := m.baseClears[ref.PodOrdinal]
	if !found {
		return false
	}
	if clearState.all {
		return true
	}
	rank, hasRank := ref.DataParallelRank()
	if !hasRank {
		return false
	}
	_, hidden := clearState.ranks[rank]
	return hidden
}

// requestKeysLocked returns the mapping's request keys. An overlay hit
// returns the LRU-owned slice: a replacement installs a new slice rather
// than mutating it in place, so callers can read it without mutationMu.
func (m *InMemoryIndex) requestKeysLocked(engineKey BlockHash, dst []BlockHash) ([]BlockHash, bool) {
	if requestKeys, found := m.engineToRequestKeys.Get(engineKey); found {
		return requestKeys, true
	}
	if m.base == nil {
		return dst, false
	}
	return m.base.requestKeys(engineKey, dst)
}

func (m *InMemoryIndex) removeBaseRequestKeyLocked(key BlockHash) {
	if m.base == nil {
		return
	}
	delete(m.base.requestOffsets, key)
}

func (m *InMemoryIndex) removeColdBaseRequestKeyLocked() bool {
	if m.base == nil {
		return false
	}
	// The mapped generation has no per-key recency data, so it is the cold generation.
	for key := range m.base.requestOffsets {
		m.removeBaseRequestKeyLocked(key)
		return true
	}
	return false
}

func (m *InMemoryIndex) addRequestRecordsLocked(key BlockHash, records []slabRef) error {
	if m.base == nil {
		evicted, didEvict, err := m.data.addTracked(key, records)
		if didEvict {
			m.removeBaseRequestKeyLocked(evicted)
		}
		return err
	}
	if _, baseFound := m.base.requestOffsets[key]; baseFound {
		merged, _ := m.mergedCompactEntriesLocked(key, nil)
		combined := make([]slabRef, 0, len(merged)+len(records))
		combined = append(combined, merged...)
		combined = append(combined, records...)
		evicted, didEvict, err := m.data.addTracked(key, combined)
		if err != nil {
			return err
		}
		m.removeBaseRequestKeyLocked(key)
		if didEvict {
			m.removeBaseRequestKeyLocked(evicted)
		}
		return nil
	}
	if _, _, overlayFound := m.data.capture(key, false); !overlayFound &&
		len(m.base.requestOffsets)+m.data.len >= m.data.capacity {
		m.removeColdBaseRequestKeyLocked()
	}
	evicted, didEvict, err := m.data.addTracked(key, records)
	if didEvict {
		m.removeBaseRequestKeyLocked(evicted)
	}
	return err
}

func (m *InMemoryIndex) addEngineMappingLocked(key BlockHash, requestKeys []BlockHash) {
	_, overlayFound := m.engineToRequestKeys.Peek(key)
	baseFound := false
	if m.base != nil {
		_, baseFound = m.base.engineOffsets[key]
		if baseFound {
			delete(m.base.engineOffsets, key)
		}
	}
	if !overlayFound && !baseFound && m.base != nil &&
		len(m.base.engineOffsets)+m.engineToRequestKeys.Len() >= m.data.capacity {
		for coldKey := range m.base.engineOffsets {
			delete(m.base.engineOffsets, coldKey)
			break
		}
	}
	oldest, willEvict := BlockHash(0), false
	if !overlayFound && m.engineToRequestKeys.Len() == m.data.capacity {
		oldest, _, willEvict = m.engineToRequestKeys.GetOldest()
	}
	if m.engineToRequestKeys.Add(key, requestKeys) && willEvict && m.base != nil {
		delete(m.base.engineOffsets, oldest)
	}
}

func (m *InMemoryIndex) clearBasePodLocked(pod uint32, dataParallelRank *int) {
	if m.base == nil {
		return
	}
	state := m.baseClears[pod]
	if dataParallelRank == nil {
		state.all = true
		state.ranks = nil
	} else if !state.all {
		if state.ranks == nil {
			state.ranks = make(map[int]struct{})
		}
		state.ranks[*dataParallelRank] = struct{}{}
	}
	m.baseClears[pod] = state
	refs := make([]CompactEntryRef, 0, int(m.data.entryCap))
	for key := range m.base.requestOffsets {
		refs, _ = m.base.compactEntries(key, refs[:0])
		visible := false
		for _, ref := range refs {
			if !m.baseRefHiddenLocked(ref) {
				visible = true
				break
			}
		}
		if !visible {
			m.removeBaseRequestKeyLocked(key)
		}
	}
}
