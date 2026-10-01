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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

type mappedSnapshot struct {
	mapping        []byte
	data           []byte
	pods           []string
	tiers          []string
	requestOffsets map[BlockHash]uint64
	engineOffsets  map[BlockHash]uint64
}

func openMappedSnapshot(file *os.File, offset, length int64, capacity, entryCap int) (*mappedSnapshot, error) {
	if offset < 0 || length < 0 || offset > math.MaxInt64-length {
		return nil, errors.New("invalid mapped snapshot range")
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat mapped snapshot: %w", err)
	}
	if offset+length > info.Size() || info.Size() > math.MaxInt {
		return nil, errors.New("mapped snapshot range exceeds the file")
	}
	mapping, err := unix.Mmap(int(file.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("map checkpoint: %w", err)
	}
	snapshot := &mappedSnapshot{mapping: mapping, data: mapping[offset : offset+length]}
	if err := snapshot.parse(capacity, entryCap); err != nil {
		_ = unix.Munmap(mapping)
		return nil, err
	}
	return snapshot, nil
}

func (s *mappedSnapshot) close() {
	if s == nil || s.mapping == nil {
		return
	}
	_ = unix.Munmap(s.mapping)
	s.mapping = nil
	s.data = nil
}

func (s *mappedSnapshot) parse(capacity, entryCap int) error {
	cursor := mappedCursor{data: s.data}
	version, err := cursor.uint32()
	if err != nil {
		return fmt.Errorf("read index snapshot version: %w", err)
	}
	if version != indexSnapshotVersion {
		return fmt.Errorf("unsupported index snapshot version %d", version)
	}
	s.pods, err = cursor.stringTable(maxInternedPods)
	if err != nil {
		return fmt.Errorf("read pod table: %w", err)
	}
	s.tiers, err = cursor.stringTable(maxInternedTiers)
	if err != nil {
		return fmt.Errorf("read device-tier table: %w", err)
	}
	nodeCount, err := cursor.uint64()
	if err != nil {
		return fmt.Errorf("read request-key count: %w", err)
	}
	if nodeCount > uint64(capacity) || nodeCount > uint64(cursor.remaining()/10) {
		return fmt.Errorf("invalid request-key count %d", nodeCount)
	}
	s.requestOffsets = make(map[BlockHash]uint64, nodeCount)
	for range nodeCount {
		key, err := cursor.uint64()
		if err != nil {
			return fmt.Errorf("read request key: %w", err)
		}
		requestKey := BlockHash(key)
		if requestKey == EmptyBlockHash {
			return errors.New("snapshot contains an empty request key")
		}
		if _, found := s.requestOffsets[requestKey]; found {
			return fmt.Errorf("snapshot contains duplicate request key %s", requestKey.String())
		}
		s.requestOffsets[requestKey] = uint64(cursor.position)
		count, err := cursor.uint16()
		if err != nil {
			return fmt.Errorf("read reference count: %w", err)
		}
		if count == 0 || int(count) > entryCap || int(count) > cursor.remaining()/20 {
			return fmt.Errorf("invalid reference count %d", count)
		}
		seen := make(map[CompactEntryRef]struct{}, min(int(count), 16))
		for range count {
			ref, err := cursor.compactRef(s.pods, s.tiers)
			if err != nil {
				return err
			}
			if _, found := seen[ref]; found {
				return errors.New("snapshot contains a duplicate cache location")
			}
			seen[ref] = struct{}{}
		}
	}
	mappingCount, err := cursor.uint64()
	if err != nil {
		return fmt.Errorf("read engine-key count: %w", err)
	}
	if mappingCount > uint64(capacity) || mappingCount > uint64(cursor.remaining()/12) {
		return fmt.Errorf("invalid engine-key count %d", mappingCount)
	}
	s.engineOffsets = make(map[BlockHash]uint64, mappingCount)
	for range mappingCount {
		key, err := cursor.uint64()
		if err != nil {
			return fmt.Errorf("read engine key: %w", err)
		}
		engineKey := BlockHash(key)
		if engineKey == EmptyBlockHash {
			return errors.New("snapshot contains an empty engine key")
		}
		if _, found := s.engineOffsets[engineKey]; found {
			return fmt.Errorf("snapshot contains duplicate engine key %s", engineKey.String())
		}
		s.engineOffsets[engineKey] = uint64(cursor.position)
		count, err := cursor.uint32()
		if err != nil {
			return fmt.Errorf("read mapping size for engine key %s: %w", engineKey.String(), err)
		}
		if uint64(count) > uint64(cursor.remaining()/8) {
			return fmt.Errorf("invalid mapping size for engine key %s", engineKey.String())
		}
		seen := make(map[BlockHash]struct{}, min(int(count), 16))
		for range count {
			value, err := cursor.uint64()
			if err != nil {
				return fmt.Errorf("read mapping for engine key %s: %w", engineKey.String(), err)
			}
			requestKey := BlockHash(value)
			if _, found := s.requestOffsets[requestKey]; !found {
				return fmt.Errorf("engine key %s references missing request key %s", engineKey.String(), requestKey.String())
			}
			if _, found := seen[requestKey]; found {
				return fmt.Errorf("engine key %s contains duplicate request key %s", engineKey.String(), requestKey.String())
			}
			seen[requestKey] = struct{}{}
		}
	}
	if cursor.remaining() != 0 {
		return errors.New("snapshot contains trailing data")
	}
	return nil
}

func (s *mappedSnapshot) compactEntries(key BlockHash, dst []CompactEntryRef) ([]CompactEntryRef, bool) {
	offset, found := s.requestOffsets[key]
	if !found {
		return dst, false
	}
	cursor := mappedCursor{data: s.data, position: int(offset)}
	count, _ := cursor.uint16()
	for range count {
		ref, _ := cursor.compactRef(s.pods, s.tiers)
		dst = append(dst, ref)
	}
	return dst, true
}

func (s *mappedSnapshot) requestKeys(engineKey BlockHash, dst []BlockHash) ([]BlockHash, bool) {
	offset, found := s.engineOffsets[engineKey]
	if !found {
		return dst, false
	}
	cursor := mappedCursor{data: s.data, position: int(offset)}
	count, _ := cursor.uint32()
	for range count {
		key, _ := cursor.uint64()
		dst = append(dst, BlockHash(key))
	}
	return dst, true
}

type mappedCursor struct {
	data     []byte
	position int
}

func (c *mappedCursor) remaining() int { return len(c.data) - c.position }

func (c *mappedCursor) bytes(length int) ([]byte, error) {
	if length < 0 || length > c.remaining() {
		return nil, io.ErrUnexpectedEOF
	}
	value := c.data[c.position : c.position+length]
	c.position += length
	return value, nil
}

func (c *mappedCursor) uint16() (uint16, error) {
	value, err := c.bytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(value), nil
}

func (c *mappedCursor) uint32() (uint32, error) {
	value, err := c.bytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value), nil
}

func (c *mappedCursor) uint64() (uint64, error) {
	value, err := c.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (c *mappedCursor) stringTable(limit int) ([]string, error) {
	count, err := c.uint32()
	if err != nil {
		return nil, err
	}
	if uint64(count) > uint64(min(limit, maxSnapshotTableItems)) || uint64(count) > uint64(c.remaining()/4) {
		return nil, fmt.Errorf("invalid string-table size %d", count)
	}
	values := make([]string, 0, min(int(count), 4096))
	seen := make(map[string]struct{}, min(int(count), 4096))
	for range count {
		length, err := c.uint32()
		if err != nil {
			return nil, err
		}
		if length > maxCheckpointString || uint64(length) > uint64(c.remaining()) {
			return nil, fmt.Errorf("invalid checkpoint string size %d", length)
		}
		encoded, _ := c.bytes(int(length))
		value := string(encoded)
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

func (c *mappedCursor) compactRef(pods, tiers []string) (CompactEntryRef, error) {
	encoded, err := c.bytes(20)
	if err != nil {
		return CompactEntryRef{}, fmt.Errorf("read cache location: %w", err)
	}
	podOrdinal := binary.LittleEndian.Uint32(encoded[0:4])
	tierOrdinal := binary.LittleEndian.Uint32(encoded[4:8])
	flags := binary.LittleEndian.Uint32(encoded[8:12])
	group := int32(binary.LittleEndian.Uint32(encoded[12:16]))
	rank := int32(binary.LittleEndian.Uint32(encoded[16:20]))
	if flags > math.MaxUint16 || uint16(flags)&^checkpointKnownFlags != 0 ||
		uint64(podOrdinal) >= uint64(len(pods)) || uint64(tierOrdinal) >= uint64(len(tiers)) {
		return CompactEntryRef{}, errors.New("snapshot contains an invalid cache location")
	}
	ref := CompactEntryRef{PodOrdinal: podOrdinal, tierAndFlags: tierOrdinal}
	if uint16(flags)&checkpointHasGroup != 0 {
		if group < 0 {
			return CompactEntryRef{}, errors.New("snapshot contains a negative group index")
		}
		ref.tierAndFlags |= hasGroupBit
		ref.group = GroupID(group)
	} else if group != -1 {
		return CompactEntryRef{}, errors.New("snapshot contains a group value without a group flag")
	}
	if uint16(flags)&checkpointHasRank != 0 {
		if rank < 0 {
			return CompactEntryRef{}, errors.New("snapshot contains a negative data-parallel rank")
		}
		ref.tierAndFlags |= hasDataParallelRankBit
		ref.dataParallelRank = int(rank)
	} else if rank != -1 {
		return CompactEntryRef{}, errors.New("snapshot contains a rank value without a rank flag")
	}
	return ref, nil
}
