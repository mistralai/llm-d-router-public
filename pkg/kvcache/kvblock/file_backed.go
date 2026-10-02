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
	v := m.writerView()
	capacity, entryCap := v.data.capacity, int(v.data.entryCap)
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
	replacementView := replacement.writerView()
	restoreInterner(replacementView.pods, base.pods)
	restoreInterner(replacementView.tiers, base.tiers)
	return &preparedMappedSnapshot{index: m, base: base, replacement: replacement}, nil
}

func (p *preparedMappedSnapshot) Install() error {
	if p.base == nil || p.installed {
		return os.ErrInvalid
	}
	m := p.index
	replacementView := p.replacement.writerView()
	next := &indexView{
		data:                replacementView.data,
		engineToRequestKeys: replacementView.engineToRequestKeys,
		pods:                replacementView.pods,
		tiers:               replacementView.tiers,
		base:                p.base,
	}
	m.baseStateMu.Lock()
	m.publishView(next)
	m.baseClears = make(map[uint32]baseClear)
	m.baseStateMu.Unlock()
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
func (m *InMemoryIndex) mergedCompactEntriesLocked(
	v *indexView, key BlockHash, dst []CompactEntryRef,
) ([]CompactEntryRef, bool) {
	dst = dst[:0]
	if v.base != nil {
		baseEntries, found := v.base.compactEntries(key, dst)
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
	return overlayCompactEntries(v, key, dst)
}

func overlayCompactEntries(v *indexView, key BlockHash, dst []CompactEntryRef) ([]CompactEntryRef, bool) {
	node, version, found := v.data.capture(key, false)
	if !found {
		return dst, false
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.hash != key || node.version != version {
		return dst, false
	}
	return append(dst, v.data.refs(node.head, node.runCap)[:node.runLen]...), true
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
// than mutating it in place, so callers can read it without a mutation lock.
func (m *InMemoryIndex) requestKeysLocked(v *indexView, engineKey BlockHash, dst []BlockHash) ([]BlockHash, bool) {
	if requestKeys, found := v.engineToRequestKeys.Get(engineKey); found {
		return requestKeys, true
	}
	if v.base == nil {
		return dst, false
	}
	return v.base.requestKeys(engineKey, dst)
}

func (m *InMemoryIndex) removeBaseRequestKeyLocked(v *indexView, key BlockHash) {
	if v.base == nil {
		return
	}
	delete(v.base.requestOffsets, key)
}

func (m *InMemoryIndex) removeColdBaseRequestKeyLocked(v *indexView) bool {
	if v.base == nil {
		return false
	}
	// The mapped generation has no per-key recency data, so it is the cold generation.
	for key := range v.base.requestOffsets {
		m.removeBaseRequestKeyLocked(v, key)
		return true
	}
	return false
}

func (m *InMemoryIndex) addRequestRecordsLocked(v *indexView, key BlockHash, records []slabRef) error {
	if v.base == nil {
		evicted, didEvict, err := v.data.addTracked(key, records)
		if didEvict {
			m.removeBaseRequestKeyLocked(v, evicted)
		}
		return err
	}
	if _, baseFound := v.base.requestOffsets[key]; baseFound {
		merged, _ := m.mergedCompactEntriesLocked(v, key, nil)
		combined := make([]slabRef, 0, len(merged)+len(records))
		combined = append(combined, merged...)
		combined = append(combined, records...)
		evicted, didEvict, err := v.data.addTracked(key, combined)
		if err != nil {
			return err
		}
		m.removeBaseRequestKeyLocked(v, key)
		if didEvict {
			m.removeBaseRequestKeyLocked(v, evicted)
		}
		return nil
	}
	if _, _, overlayFound := v.data.capture(key, false); !overlayFound &&
		len(v.base.requestOffsets)+v.data.len >= v.data.capacity {
		m.removeColdBaseRequestKeyLocked(v)
	}
	evicted, didEvict, err := v.data.addTracked(key, records)
	if didEvict {
		m.removeBaseRequestKeyLocked(v, evicted)
	}
	return err
}

func (m *InMemoryIndex) addEngineMappingLocked(v *indexView, key BlockHash, requestKeys []BlockHash) {
	_, overlayFound := v.engineToRequestKeys.Peek(key)
	baseFound := false
	if v.base != nil {
		_, baseFound = v.base.engineOffsets[key]
		if baseFound {
			delete(v.base.engineOffsets, key)
		}
	}
	if !overlayFound && !baseFound && v.base != nil &&
		len(v.base.engineOffsets)+v.engineToRequestKeys.Len() >= v.data.capacity {
		for coldKey := range v.base.engineOffsets {
			delete(v.base.engineOffsets, coldKey)
			break
		}
	}
	oldest, willEvict := BlockHash(0), false
	if !overlayFound && v.engineToRequestKeys.Len() == v.data.capacity {
		oldest, _, willEvict = v.engineToRequestKeys.GetOldest()
	}
	if v.engineToRequestKeys.Add(key, requestKeys) && willEvict && v.base != nil {
		delete(v.base.engineOffsets, oldest)
	}
}

func (m *InMemoryIndex) clearBasePodLocked(v *indexView, pod uint32, dataParallelRank *int) {
	if v.base == nil {
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
	refs := make([]CompactEntryRef, 0, int(v.data.entryCap))
	for key := range v.base.requestOffsets {
		refs, _ = v.base.compactEntries(key, refs[:0])
		visible := false
		for _, ref := range refs {
			if !m.baseRefHiddenLocked(ref) {
				visible = true
				break
			}
		}
		if !visible {
			m.removeBaseRequestKeyLocked(v, key)
		}
	}
}
