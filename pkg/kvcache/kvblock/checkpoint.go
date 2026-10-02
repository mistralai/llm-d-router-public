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

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

const (
	indexSnapshotVersion  = 1
	maxCheckpointString   = 1 << 20
	maxSnapshotTableItems = 1 << 20
	checkpointHasGroup    = uint16(1 << 0)
	checkpointHasRank     = uint16(1 << 1)
	checkpointKnownFlags  = checkpointHasGroup | checkpointHasRank
)

// Snapshotter streams index checkpoints without constructing a second index.
type Snapshotter interface {
	WriteSnapshot(io.Writer) error
	RestoreSnapshot(io.Reader) error
}

// FileSnapshotter can retain a file-backed snapshot instead of copying it.
type FileSnapshotter interface {
	Snapshotter
	RestoreSnapshotFile(file *os.File, offset, length int64) error
	PrepareSnapshotFile(file *os.File, offset, length int64) (PreparedFileSnapshot, error)
}

// PreparedFileSnapshot is validated state that can replace the live view.
type PreparedFileSnapshot interface {
	Install() error
	Close()
}

type snapshotterForwarder struct{ Snapshotter }

type fileSnapshotterForwarder struct{ FileSnapshotter }

var _ Snapshotter = &InMemoryIndex{}
var _ FileSnapshotter = &InMemoryIndex{}

// WriteSnapshot writes confirmed entries and their engine-key mappings.
// It runs on the writer goroutine, where it cannot interleave with another
// mutation.
func (m *InMemoryIndex) WriteSnapshot(dst io.Writer) error {
	v := m.writerView()
	w := bufio.NewWriterSize(dst, 1<<20)
	encoder := snapshotEncoder{writer: w}
	if err := encoder.writeUint32(indexSnapshotVersion); err != nil {
		return err
	}
	if err := writeInterner(&encoder, v.pods); err != nil {
		return fmt.Errorf("write pod table: %w", err)
	}
	if err := writeInterner(&encoder, v.tiers); err != nil {
		return fmt.Errorf("write device-tier table: %w", err)
	}

	requestKeyCount := uint64(0)
	if err := m.forEachSnapshotRequestLocked(v, func(BlockHash, []CompactEntryRef) error {
		requestKeyCount++
		return nil
	}); err != nil {
		return err
	}
	if err := encoder.writeUint64(requestKeyCount); err != nil {
		return err
	}
	if err := m.forEachSnapshotRequestLocked(v, func(requestKey BlockHash, refs []CompactEntryRef) error {
		if err := encoder.writeUint64(uint64(requestKey)); err != nil {
			return err
		}
		confirmed := 0
		for _, ref := range refs {
			if !ref.Speculative() {
				confirmed++
			}
		}
		if err := encoder.writeUint16(uint16(confirmed)); err != nil { // #nosec G115 -- entryCap is uint16.
			return err
		}
		for _, ref := range refs {
			if !ref.Speculative() {
				if err := writeCheckpointRef(&encoder, ref); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	engineMappingCount := m.countSnapshotEngineMappings(v)
	if err := encoder.writeUint64(engineMappingCount); err != nil {
		return err
	}
	if err := m.forEachSnapshotEngineMappingLocked(v, func(engineKey BlockHash, mappedRequestKeys []BlockHash) error {
		if err := encoder.writeUint64(uint64(engineKey)); err != nil {
			return err
		}
		if uint64(len(mappedRequestKeys)) > math.MaxUint32 {
			return errors.New("engine mapping is too large")
		}
		if err := encoder.writeUint32(uint32(len(mappedRequestKeys))); err != nil { // #nosec G115 -- checked above.
			return err
		}
		for _, requestKey := range mappedRequestKeys {
			if err := encoder.writeUint64(uint64(requestKey)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush index checkpoint: %w", err)
	}
	return nil
}

// snapshotMergedEntries reads one generation without the slab or node locks.
// The snapshot passes run on the writer goroutine, where no mutation can
// interleave, so the slab, the base maps, and baseClears are stable for the
// whole write and reads need no locking. Concurrent lookups only read node
// contents; their recency promotion touches the LRU links, which this path
// never reads.
func (m *InMemoryIndex) snapshotMergedEntries(v *indexView, key BlockHash, dst []CompactEntryRef) ([]CompactEntryRef, bool) {
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
	return snapshotOverlayEntries(v, key, dst)
}

func snapshotOverlayEntries(v *indexView, key BlockHash, dst []CompactEntryRef) ([]CompactEntryRef, bool) {
	id, found := v.data.items[key]
	if !found {
		return dst, false
	}
	node := v.data.node(id)
	if node.runCap == 0 || node.hash != key {
		return dst, false
	}
	return append(dst, v.data.refs(node.head, node.runCap)[:node.runLen]...), true
}

// snapshotAnyConfirmed reports whether any of the mapped request keys holds a
// confirmed entry. It backs the engine-mapping count pass, which needs no
// filtered key list.
func (m *InMemoryIndex) snapshotAnyConfirmed(v *indexView, requestKeys []BlockHash, scratch []CompactEntryRef) bool {
	for _, key := range requestKeys {
		refs, found := m.snapshotMergedEntries(v, key, scratch[:0])
		if found && hasConfirmedRef(refs) {
			return true
		}
	}
	return false
}

func (m *InMemoryIndex) forEachSnapshotRequestLocked(
	v *indexView,
	visit func(BlockHash, []CompactEntryRef) error,
) error {
	refs := make([]CompactEntryRef, 0, int(v.data.entryCap))
	if v.base != nil {
		for key := range v.base.requestOffsets {
			var found bool
			refs, found = m.snapshotMergedEntries(v, key, refs)
			if found && hasConfirmedRef(refs) {
				if err := visit(key, refs); err != nil {
					return err
				}
			}
		}
	}
	nextNode := v.data.nextNode
	for rawID := uint64(1); rawID < nextNode; rawID++ {
		id := nodeID(rawID) // #nosec G115 -- the slab limits node identifiers to uint32.
		node := v.data.node(id)
		if node.runCap == 0 {
			continue
		}
		key := node.hash
		if v.base != nil {
			if _, duplicate := v.base.requestOffsets[key]; duplicate {
				continue
			}
		}
		nodeRefs := v.data.refs(node.head, node.runCap)[:node.runLen]
		if hasConfirmedRef(nodeRefs) {
			if err := visit(key, nodeRefs); err != nil {
				return err
			}
		}
	}
	return nil
}

// countSnapshotEngineMappings counts the engine keys whose mappings survive
// the confirmed filter. It probes request keys instead of building filtered
// lists, so the count prefix costs one pass of existence checks.
func (m *InMemoryIndex) countSnapshotEngineMappings(v *indexView) uint64 {
	count := uint64(0)
	requestKeys := make([]BlockHash, 0)
	scratch := make([]CompactEntryRef, 0, int(v.data.entryCap))
	if v.base != nil {
		for engineKey := range v.base.engineOffsets {
			if _, shadowed := v.engineToRequestKeys.Peek(engineKey); shadowed {
				continue
			}
			requestKeys = requestKeys[:0]
			requestKeys, _ = v.base.requestKeys(engineKey, requestKeys)
			if m.snapshotAnyConfirmed(v, requestKeys, scratch) {
				count++
			}
		}
	}
	for _, engineKey := range v.engineToRequestKeys.Keys() {
		mapped, found := v.engineToRequestKeys.Peek(engineKey)
		if !found {
			continue
		}
		if m.snapshotAnyConfirmed(v, mapped, scratch) {
			count++
		}
	}
	return count
}

func (m *InMemoryIndex) forEachSnapshotEngineMappingLocked(
	v *indexView,
	visit func(BlockHash, []BlockHash) error,
) error {
	requestKeys := make([]BlockHash, 0)
	validRequestKeys := make([]BlockHash, 0)
	refBuffer := make([]CompactEntryRef, 0, int(v.data.entryCap))
	if v.base != nil {
		for engineKey := range v.base.engineOffsets {
			if _, shadowed := v.engineToRequestKeys.Peek(engineKey); shadowed {
				continue
			}
			requestKeys = requestKeys[:0]
			requestKeys, _ = v.base.requestKeys(engineKey, requestKeys)
			validRequestKeys, refBuffer = m.confirmedRequestKeysLocked(
				v, requestKeys, validRequestKeys[:0], refBuffer,
			)
			if len(validRequestKeys) != 0 {
				if err := visit(engineKey, validRequestKeys); err != nil {
					return err
				}
			}
		}
	}
	for _, engineKey := range v.engineToRequestKeys.Keys() {
		mapped, found := v.engineToRequestKeys.Peek(engineKey)
		if !found {
			continue
		}
		validRequestKeys, refBuffer = m.confirmedRequestKeysLocked(
			v, mapped, validRequestKeys[:0], refBuffer,
		)
		if len(validRequestKeys) != 0 {
			if err := visit(engineKey, validRequestKeys); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *InMemoryIndex) confirmedRequestKeysLocked(
	v *indexView,
	requestKeys, dst []BlockHash, refBuffer []CompactEntryRef,
) ([]BlockHash, []CompactEntryRef) {
	for _, requestKey := range requestKeys {
		var found bool
		refBuffer, found = m.snapshotMergedEntries(v, requestKey, refBuffer[:0])
		if found && hasConfirmedRef(refBuffer) {
			dst = append(dst, requestKey)
		}
	}
	return dst, refBuffer
}

func hasConfirmedRef(refs []CompactEntryRef) bool {
	for _, ref := range refs {
		if !ref.Speculative() {
			return true
		}
	}
	return false
}

func writeInterner(w *snapshotEncoder, in *interner) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	if len(in.ids) > maxSnapshotTableItems {
		return errors.New("snapshot string table is too large")
	}
	if err := w.writeUint32(uint32(len(in.ids))); err != nil { // #nosec G115 -- the interner has a smaller fixed limit.
		return err
	}
	for ordinal := range len(in.ids) {
		if err := w.writeString(in.name(uint32(ordinal))); err != nil { // #nosec G115 -- the interner has a smaller fixed limit.
			return err
		}
	}
	return nil
}

func writeCheckpointRef(w *snapshotEncoder, ref CompactEntryRef) error {
	flags, group, rank := uint16(0), int32(-1), int32(-1)
	if ref.HasGroup() {
		if ref.GroupIdx() < 0 || int64(ref.GroupIdx()) > math.MaxInt32 {
			return fmt.Errorf("group index %d cannot be stored", ref.GroupIdx())
		}
		flags |= checkpointHasGroup
		group = int32(ref.GroupIdx()) // #nosec G115 -- checked above.
	}
	if value, present := ref.DataParallelRank(); present {
		if value < 0 || int64(value) > math.MaxInt32 {
			return fmt.Errorf("data-parallel rank %d cannot be stored", value)
		}
		flags |= checkpointHasRank
		rank = int32(value) // #nosec G115 -- checked above.
	}
	for _, value := range []uint32{ref.PodOrdinal, ref.TierOrdinal(), uint32(flags), uint32(group), uint32(rank)} {
		if err := w.writeUint32(value); err != nil {
			return err
		}
	}
	return nil
}

// RestoreSnapshot builds a replacement index and swaps it in after validation.
// It runs on the writer goroutine; concurrent readers keep the previous view
// until the publish.
func (m *InMemoryIndex) RestoreSnapshot(src io.Reader) error {
	v := m.writerView()
	r := newSnapshotReader(src)
	version, err := r.readUint32()
	if err != nil {
		return fmt.Errorf("read index snapshot version: %w", err)
	}
	if version != indexSnapshotVersion {
		return fmt.Errorf("unsupported index snapshot version %d", version)
	}
	pods, err := readStringTable(r, maxInternedPods)
	if err != nil {
		return fmt.Errorf("read pod table: %w", err)
	}
	tiers, err := readStringTable(r, maxInternedTiers)
	if err != nil {
		return fmt.Errorf("read device-tier table: %w", err)
	}
	replacement, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: v.data.capacity, PodCacheSize: int(v.data.entryCap)})
	if err != nil {
		return fmt.Errorf("create replacement index: %w", err)
	}
	nodeCount, err := r.readUint64()
	if err != nil {
		return fmt.Errorf("read request-key count: %w", err)
	}
	if nodeCount > uint64(v.data.capacity) || nodeCount > r.remaining()/10 {
		return fmt.Errorf("invalid request-key count %d", nodeCount)
	}
	replacementView := replacement.writerView()
	restoreInterner(replacementView.pods, pods)
	restoreInterner(replacementView.tiers, tiers)
	records := make([]slabRef, 0, min(int(v.data.entryCap), 16))
	replacementView.data.mu.Lock()
	for range nodeCount {
		requestKey, restoredRecords, readErr := readCheckpointEntry(r, pods, tiers, int(v.data.entryCap), records)
		if readErr != nil {
			replacementView.data.mu.Unlock()
			return readErr
		}
		if requestKey == EmptyBlockHash {
			replacementView.data.mu.Unlock()
			return errors.New("snapshot contains an empty request key")
		}
		if _, found := replacementView.data.items[requestKey]; found {
			replacementView.data.mu.Unlock()
			return fmt.Errorf("snapshot contains duplicate request key %s", requestKey.String())
		}
		if len(restoredRecords) == 0 {
			replacementView.data.mu.Unlock()
			return fmt.Errorf("request key %s has no confirmed cache locations", requestKey.String())
		}
		if err := replacementView.data.addNewWithStoreLockHeld(requestKey, restoredRecords); err != nil {
			replacementView.data.mu.Unlock()
			return fmt.Errorf("restore request key %s: %w", requestKey.String(), err)
		}
		records = restoredRecords[:0]
	}
	replacementView.data.mu.Unlock()
	mappingCount, err := r.readUint64()
	if err != nil {
		return fmt.Errorf("read engine-key count: %w", err)
	}
	if mappingCount > uint64(v.data.capacity) || mappingCount > r.remaining()/12 {
		return fmt.Errorf("invalid engine-key count %d", mappingCount)
	}
	seenEngineKeys := make(map[BlockHash]struct{})
	for range mappingCount {
		engineKeyValue, readErr := r.readUint64()
		if readErr != nil {
			return fmt.Errorf("read engine key: %w", readErr)
		}
		engineKey := BlockHash(engineKeyValue)
		requestKeyCount, readErr := r.readUint32()
		if readErr != nil {
			return fmt.Errorf("read mapping size for engine key %s: %w", engineKey.String(), readErr)
		}
		if uint64(requestKeyCount) > uint64(v.data.capacity) || uint64(requestKeyCount) > r.remaining()/8 {
			return fmt.Errorf("invalid mapping size for engine key %s", engineKey.String())
		}
		if engineKey == EmptyBlockHash {
			return fmt.Errorf("invalid or duplicate engine key %s", engineKey.String())
		}
		if _, found := seenEngineKeys[engineKey]; found {
			return fmt.Errorf("invalid or duplicate engine key %s", engineKey.String())
		}
		seenEngineKeys[engineKey] = struct{}{}
		requestKeys := make([]BlockHash, 0, requestKeyCount)
		seen := make(map[BlockHash]struct{}, requestKeyCount)
		for range requestKeyCount {
			value, keyErr := r.readUint64()
			if keyErr != nil {
				return fmt.Errorf("read mapping for engine key %s: %w", engineKey.String(), keyErr)
			}
			requestKey := BlockHash(value)
			if _, found := replacementView.data.items[requestKey]; !found {
				return fmt.Errorf("engine key %s references missing request key %s", engineKey.String(), requestKey.String())
			}
			if _, found := seen[requestKey]; found {
				return fmt.Errorf("engine key %s contains duplicate request key %s", engineKey.String(), requestKey.String())
			}
			seen[requestKey] = struct{}{}
			requestKeys = append(requestKeys, requestKey)
		}
		if len(requestKeys) > 0 {
			replacementView.engineToRequestKeys.Add(engineKey, requestKeys)
		}
	}
	if err := requireEOF(r); err != nil {
		return err
	}
	m.baseStateMu.Lock()
	m.publishView(&indexView{
		data:                replacementView.data,
		engineToRequestKeys: replacementView.engineToRequestKeys,
		pods:                replacementView.pods,
		tiers:               replacementView.tiers,
	})
	m.baseClears = make(map[uint32]baseClear)
	m.baseStateMu.Unlock()
	return nil
}

func restoreInterner(in *interner, values []string) {
	for _, value := range values {
		in.internLocked(value)
	}
}

func readCheckpointEntry(
	r *snapshotReader,
	pods, tiers []string,
	entryCap int,
	records []slabRef,
) (BlockHash, []slabRef, error) {
	key, err := r.readUint64()
	if err != nil {
		return 0, nil, fmt.Errorf("read request key: %w", err)
	}
	count, err := r.readUint16()
	if err != nil {
		return 0, nil, fmt.Errorf("read reference count: %w", err)
	}
	if int(count) > entryCap || uint64(count) > r.remaining()/20 {
		return 0, nil, fmt.Errorf("invalid reference count %d", count)
	}
	records = records[:0]
	for range count {
		var values [5]uint32
		for i := range values {
			values[i], err = r.readUint32()
			if err != nil {
				return 0, nil, fmt.Errorf("read cache location: %w", err)
			}
		}
		podOrdinal, tierOrdinal, flags := values[0], values[1], uint16(values[2])
		group, rank := int32(values[3]), int32(values[4])
		if values[2] > math.MaxUint16 || flags&^checkpointKnownFlags != 0 ||
			uint64(podOrdinal) >= uint64(len(pods)) || uint64(tierOrdinal) >= uint64(len(tiers)) {
			return 0, nil, errors.New("snapshot contains an invalid cache location")
		}
		record := slabRef{PodOrdinal: podOrdinal, tierAndFlags: tierOrdinal}
		if flags&checkpointHasGroup != 0 {
			if group < 0 {
				return 0, nil, errors.New("snapshot contains a negative group index")
			}
			record.tierAndFlags |= hasGroupBit
			record.group = GroupID(group)
		} else if group != -1 {
			return 0, nil, errors.New("snapshot contains a group value without a group flag")
		}
		if flags&checkpointHasRank != 0 {
			if rank < 0 {
				return 0, nil, errors.New("snapshot contains a negative data-parallel rank")
			}
			record.tierAndFlags |= hasDataParallelRankBit
			record.dataParallelRank = int(rank)
		} else if rank != -1 {
			return 0, nil, errors.New("snapshot contains a rank value without a rank flag")
		}
		for _, existing := range records {
			if existing == record {
				return 0, nil, errors.New("snapshot contains a duplicate cache location")
			}
		}
		records = append(records, record)
	}
	return BlockHash(key), records, nil
}

func readStringTable(r *snapshotReader, limit int) ([]string, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("read string-table size: %w", err)
	}
	if uint64(count) > uint64(min(limit, maxSnapshotTableItems)) || uint64(count) > r.remaining()/4 {
		return nil, fmt.Errorf("invalid string-table size %d", count)
	}
	initialCapacity := min(int(count), 4096)
	values := make([]string, 0, initialCapacity)
	seen := make(map[string]struct{}, initialCapacity)
	for range count {
		value, readErr := r.readString()
		if readErr != nil {
			return nil, readErr
		}
		if value == "" {
			return nil, errors.New("string table contains an empty value")
		}
		if _, found := seen[value]; found {
			return nil, fmt.Errorf("string table contains duplicate value %q", value)
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values, nil
}

func writeString(w io.Writer, value string) error {
	if len(value) > maxCheckpointString {
		return fmt.Errorf("checkpoint string is too long: %d bytes", len(value))
	}
	if err := writeUint32(w, uint32(len(value))); err != nil { // #nosec G115 -- checked above.
		return err
	}
	_, err := io.WriteString(w, value)
	return err
}

type snapshotEncoder struct {
	writer  io.Writer
	scratch [8]byte
}

func (w *snapshotEncoder) writeString(value string) error {
	if len(value) > maxCheckpointString {
		return fmt.Errorf("checkpoint string is too long: %d bytes", len(value))
	}
	if err := w.writeUint32(uint32(len(value))); err != nil { // #nosec G115 -- checked above.
		return err
	}
	_, err := io.WriteString(w.writer, value)
	return err
}

func (w *snapshotEncoder) writeUint16(value uint16) error {
	binary.LittleEndian.PutUint16(w.scratch[:2], value)
	_, err := w.writer.Write(w.scratch[:2])
	return err
}

func (w *snapshotEncoder) writeUint32(value uint32) error {
	binary.LittleEndian.PutUint32(w.scratch[:4], value)
	_, err := w.writer.Write(w.scratch[:4])
	return err
}

func (w *snapshotEncoder) writeUint64(value uint64) error {
	binary.LittleEndian.PutUint64(w.scratch[:8], value)
	_, err := w.writer.Write(w.scratch[:8])
	return err
}

func writeUint16(w io.Writer, value uint16) error { return binary.Write(w, binary.LittleEndian, value) }
func writeUint32(w io.Writer, value uint32) error { return binary.Write(w, binary.LittleEndian, value) }
func writeUint64(w io.Writer, value uint64) error { return binary.Write(w, binary.LittleEndian, value) }

func requireEOF(r io.Reader) error {
	var extra [1]byte
	count, err := r.Read(extra[:])
	if count != 0 || err == nil {
		return errors.New("snapshot contains trailing data")
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("read snapshot trailer: %w", err)
	}
	return nil
}

type snapshotReader struct {
	reader    *bufio.Reader
	bytesLeft uint64
	scratch   [8]byte
}

func newSnapshotReader(src io.Reader) *snapshotReader {
	bytesLeft := uint64(math.MaxUint64)
	if sized, ok := src.(interface{ Size() int64 }); ok && sized.Size() >= 0 {
		bytesLeft = uint64(sized.Size())
	}
	return &snapshotReader{reader: bufio.NewReaderSize(src, 1<<20), bytesLeft: bytesLeft}
}

func (r *snapshotReader) Read(dst []byte) (int, error) {
	read, err := r.reader.Read(dst)
	if uint64(read) > r.bytesLeft {
		r.bytesLeft = 0
	} else {
		r.bytesLeft -= uint64(read)
	}
	return read, err
}

func (r *snapshotReader) remaining() uint64 {
	return r.bytesLeft
}

func (r *snapshotReader) readString() (string, error) {
	length, err := r.readUint32()
	if err != nil {
		return "", err
	}
	if length > maxCheckpointString {
		return "", fmt.Errorf("checkpoint string is too long: %d bytes", length)
	}
	value := make([]byte, length)
	if _, err := io.ReadFull(r, value); err != nil {
		return "", err
	}
	return string(value), nil
}

func (r *snapshotReader) readUint16() (uint16, error) {
	if _, err := io.ReadFull(r, r.scratch[:2]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(r.scratch[:2]), nil
}

func (r *snapshotReader) readUint32() (uint32, error) {
	if _, err := io.ReadFull(r, r.scratch[:4]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(r.scratch[:4]), nil
}

func (r *snapshotReader) readUint64() (uint64, error) {
	if _, err := io.ReadFull(r, r.scratch[:8]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(r.scratch[:8]), nil
}
