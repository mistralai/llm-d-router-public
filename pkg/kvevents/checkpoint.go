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

package kvevents

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

const (
	checkpointSchemaVersion = 2
	checkpointHeaderSize    = 192
	checkpointSectionCount  = 4
	checkpointMaxString     = 1 << 20
	checkpointMaxFileSize   = 64 << 30
	checkpointMaxTableItems = 1 << 20
	checkpointNoValue       = int64(-1)

	checkpointIndexSection   = 0
	checkpointDedupSection   = 1
	checkpointGroupsSection  = 2
	checkpointSourcesSection = 3

	checkpointFilePrefix = "checkpoint-"
	checkpointFileSuffix = ".bin"
)

var checkpointMagic = [8]byte{'L', 'L', 'M', 'D', 'K', 'V', 'C', '2'}

type checkpointSource struct {
	EventEndpoint       string
	ServingEndpoint     string
	DataParallelRank    *int
	LastAppliedSequence uint64
	EventDigest         checkpointDigest
	Generation          uint64
}

func (s checkpointSource) matches(eventEndpoint, servingEndpoint string, dataParallelRank *int) bool {
	return s.EventEndpoint == eventEndpoint && s.ServingEndpoint == servingEndpoint &&
		equalOptionalInt(s.DataParallelRank, dataParallelRank)
}

type checkpointDigest [sha256.Size]byte

func checkpointEventDigest(topic string, payload []byte) checkpointDigest {
	hash := sha256.New()
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(topic)))
	_, _ = hash.Write(size[:])
	_, _ = io.WriteString(hash, topic)
	_, _ = hash.Write(payload)
	var digest checkpointDigest
	hash.Sum(digest[:0])
	return digest
}

type checkpointSection struct {
	Offset uint64
	Length uint64
	Count  uint64
}

type checkpointHeader struct {
	CreatedAt                time.Time
	ConfigurationFingerprint [sha256.Size]byte
	BodyChecksum             [sha256.Size]byte
	Sections                 [checkpointSectionCount]checkpointSection
}

// CheckpointResult describes a completed checkpoint write.
type CheckpointResult struct {
	Path      string    `json:"path"`
	CreatedAt time.Time `json:"createdAt"`
	Sources   int       `json:"sources"`
	SizeBytes int64     `json:"sizeBytes"`
}

// CheckpointPathForWriter returns the file reserved for one checkpoint writer.
func CheckpointPathForWriter(directory, writerID string) (string, error) {
	if directory == "" {
		return "", errors.New("checkpoint directory is not configured")
	}
	if writerID == "" {
		return "", errors.New("checkpoint writer ID is empty")
	}
	digest := sha256.Sum256([]byte(writerID))
	name := checkpointFilePrefix + hex.EncodeToString(digest[:]) + checkpointFileSuffix
	return filepath.Join(directory, name), nil
}

// WriteCheckpointForWriter atomically replaces one writer's checkpoint file.
func (p *Pool) WriteCheckpointForWriter(
	directory, writerID, configurationFingerprint string,
) (CheckpointResult, error) {
	path, err := CheckpointPathForWriter(directory, writerID)
	if err != nil {
		return CheckpointResult{}, err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return CheckpointResult{}, fmt.Errorf("create checkpoint directory: %w", err)
	}
	return p.WriteCheckpoint(path, configurationFingerprint)
}

type checkpointCandidate struct {
	path       string
	modifiedAt time.Time
}

// RestoreLatestCheckpoint restores the newest valid complete writer snapshot.
func (p *Pool) RestoreLatestCheckpoint(
	directory, configurationFingerprint string,
) (string, bool, error) {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read checkpoint directory: %w", err)
	}
	candidates := make([]checkpointCandidate, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, checkpointFilePrefix) || !strings.HasSuffix(name, checkpointFileSuffix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		candidates = append(candidates, checkpointCandidate{
			path: filepath.Join(directory, name), modifiedAt: info.ModTime(),
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modifiedAt.Equal(candidates[j].modifiedAt) {
			return candidates[i].path > candidates[j].path
		}
		return candidates[i].modifiedAt.After(candidates[j].modifiedAt)
	})

	errs := make([]error, 0, len(candidates))
	for _, candidate := range candidates {
		restored, err := p.RestoreCheckpoint(candidate.path, configurationFingerprint)
		if err == nil && restored {
			return candidate.path, true, nil
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("restore %s: %w", candidate.path, err))
		}
	}
	if len(errs) > 0 {
		return "", false, errors.Join(errs...)
	}
	return "", false, nil
}

// WriteCheckpoint atomically replaces path with a consistent pool snapshot.
// While the pool runs, the write executes on the writer goroutine, so it
// lands at an exact message boundary without blocking other sources.
func (p *Pool) WriteCheckpoint(path, configurationFingerprint string) (CheckpointResult, error) {
	if path == "" {
		return CheckpointResult{}, errors.New("checkpoint path is not configured")
	}
	fingerprint, err := decodeFingerprint(configurationFingerprint)
	if err != nil {
		return CheckpointResult{}, err
	}
	snapshotter, ok := p.index.(kvblock.Snapshotter)
	if !ok {
		return CheckpointResult{}, errors.New("index backend does not support checkpoints")
	}
	_ = snapshotter // re-asserted by writeCheckpoint on the writer goroutine
	if p.started.Load() {
		type writeResult struct {
			result CheckpointResult
			err    error
		}
		done := make(chan writeResult, 1)
		delivered := p.Submit(func(ctx context.Context, index kvblock.Index) {
			result, err := p.writeCheckpoint(path, fingerprint)
			done <- writeResult{result, err}
		})
		if !delivered {
			return CheckpointResult{}, errors.New("pool is shutting down")
		}
		select {
		case r := <-done:
			return r.result, r.err
		case <-p.stopped:
			return CheckpointResult{}, errors.New("pool shut down before the checkpoint write")
		}
	}
	return p.writeCheckpoint(path, fingerprint)
}

func (p *Pool) writeCheckpoint(path string, fingerprint [sha256.Size]byte) (CheckpointResult, error) {
	snapshotter, ok := p.index.(kvblock.Snapshotter)
	if !ok {
		return CheckpointResult{}, errors.New("index backend does not support checkpoints")
	}
	directoryPath := filepath.Dir(path)
	temporary, err := os.CreateTemp(directoryPath, ".checkpoint-*")
	if err != nil {
		return CheckpointResult{}, fmt.Errorf("create checkpoint temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return CheckpointResult{}, fmt.Errorf("set checkpoint permissions: %w", err)
	}
	if _, err := temporary.Write(make([]byte, checkpointHeaderSize)); err != nil {
		temporary.Close()
		return CheckpointResult{}, fmt.Errorf("reserve checkpoint header: %w", err)
	}

	digest := sha256.New()
	buffered := bufio.NewWriterSize(io.MultiWriter(temporary, digest), 1<<20)
	writer := &checkpointWriter{
		writer: buffered, offset: checkpointHeaderSize, limit: checkpointMaxFileSize,
	}
	header := checkpointHeader{ConfigurationFingerprint: fingerprint}

	writeErr := func() error {
		p.appliedSourcesMu.RLock()
		invalidSourceCount := len(p.invalidSources)
		p.appliedSourcesMu.RUnlock()
		if invalidSourceCount != 0 {
			return fmt.Errorf("%d event sources have unapplied mutations", invalidSourceCount)
		}
		start := writer.offset
		if err := snapshotter.WriteSnapshot(writer); err != nil {
			return fmt.Errorf("write index section: %w", err)
		}
		header.Sections[checkpointIndexSection] = writer.section(start, 0)

		start = writer.offset
		count, err := p.writeDedupSection(writer)
		if err != nil {
			return err
		}
		header.Sections[checkpointDedupSection] = writer.section(start, count)

		groups := p.groupCatalog.Snapshot()
		start = writer.offset
		if err := writeGroupsSection(writer, groups); err != nil {
			return err
		}
		header.Sections[checkpointGroupsSection] = writer.section(start, uint64(len(groups)))

		p.appliedSourcesMu.RLock()
		start = writer.offset
		count, err = writeSourcesSection(writer, p.appliedSources)
		p.appliedSourcesMu.RUnlock()
		if err != nil {
			return err
		}
		header.Sections[checkpointSourcesSection] = writer.section(start, count)
		return nil
	}()
	if writeErr == nil {
		writeErr = buffered.Flush()
	}
	if writeErr != nil {
		temporary.Close()
		return CheckpointResult{}, writeErr
	}
	header.CreatedAt = time.Now().UTC()
	copy(header.BodyChecksum[:], digest.Sum(nil))
	encodedHeader := encodeCheckpointHeader(header)
	if _, err := temporary.WriteAt(encodedHeader, 0); err != nil {
		temporary.Close()
		return CheckpointResult{}, fmt.Errorf("write checkpoint header: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return CheckpointResult{}, fmt.Errorf("sync checkpoint: %w", err)
	}
	var prepared kvblock.PreparedFileSnapshot
	if fileSnapshotter, ok := p.index.(kvblock.FileSnapshotter); ok {
		indexSection := header.Sections[checkpointIndexSection]
		prepared, err = fileSnapshotter.PrepareSnapshotFile(
			temporary, int64(indexSection.Offset), int64(indexSection.Length),
		)
		if err != nil {
			temporary.Close()
			return CheckpointResult{}, fmt.Errorf("validate index rebase: %w", err)
		}
		defer prepared.Close()
	}
	if err := temporary.Close(); err != nil {
		return CheckpointResult{}, fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return CheckpointResult{}, fmt.Errorf("replace checkpoint: %w", err)
	}
	if err := syncDirectory(directoryPath); err != nil {
		return CheckpointResult{}, err
	}
	if prepared != nil {
		if err := prepared.Install(); err != nil {
			return CheckpointResult{}, fmt.Errorf("install index rebase: %w", err)
		}
	}
	return CheckpointResult{Path: path, CreatedAt: header.CreatedAt,
		Sources: int(header.Sections[checkpointSourcesSection].Count), SizeBytes: int64(writer.offset)}, nil
}

// RestoreCheckpoint restores a checkpoint before event workers and subscribers start.
func (p *Pool) RestoreCheckpoint(path, configurationFingerprint string) (bool, error) {
	if path == "" {
		return false, nil
	}
	fingerprint, err := decodeFingerprint(configurationFingerprint)
	if err != nil {
		return false, err
	}
	if p.started.Load() {
		return false, errors.New("checkpoint must be restored before the event pool starts")
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("open checkpoint: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("stat checkpoint: %w", err)
	}
	if info.Size() < checkpointHeaderSize || uint64(info.Size()) > checkpointMaxFileSize {
		return false, fmt.Errorf("invalid checkpoint size %d", info.Size())
	}
	headerBytes := make([]byte, checkpointHeaderSize)
	if _, err := io.ReadFull(file, headerBytes); err != nil {
		return false, fmt.Errorf("read checkpoint header: %w", err)
	}
	header, err := decodeCheckpointHeader(headerBytes, uint64(info.Size()))
	if err != nil {
		return false, err
	}
	if header.ConfigurationFingerprint != fingerprint {
		return false, errors.New("checkpoint configuration does not match")
	}
	if err := verifyCheckpointBody(file, uint64(info.Size()), header.BodyChecksum); err != nil {
		return false, err
	}

	dedupSection := header.Sections[checkpointDedupSection]
	dedup, err := readDedupSection(sectionReader(file, dedupSection), dedupSection)
	if err != nil {
		return false, err
	}
	groups, err := readGroupsSection(sectionReader(file, header.Sections[checkpointGroupsSection]),
		header.Sections[checkpointGroupsSection].Count)
	if err != nil {
		return false, err
	}
	sources, err := readSourcesSection(sectionReader(file, header.Sections[checkpointSourcesSection]),
		header.Sections[checkpointSourcesSection].Count)
	if err != nil {
		return false, err
	}
	snapshotter, ok := p.index.(kvblock.Snapshotter)
	if !ok {
		return false, errors.New("index backend does not support checkpoints")
	}
	indexSection := header.Sections[checkpointIndexSection]
	var restoreErr error
	if fileSnapshotter, ok := p.index.(kvblock.FileSnapshotter); ok {
		restoreErr = fileSnapshotter.RestoreSnapshotFile(
			file, int64(indexSection.Offset), int64(indexSection.Length),
		)
	} else {
		restoreErr = snapshotter.RestoreSnapshot(sectionReader(file, indexSection))
	}
	if restoreErr != nil {
		return false, fmt.Errorf("restore index: %w", restoreErr)
	}
	p.dedup = dedup
	p.groupCatalog.Restore(groups)
	p.appliedSourcesMu.Lock()
	p.appliedSources = sources
	p.invalidSources = make(map[string]map[uint64]struct{})
	p.sourceGenerations = make(map[string]uint64)
	p.retiredGenerations = make(map[string]map[uint64]struct{})
	p.appliedSourcesMu.Unlock()
	return true, nil
}

func (p *Pool) writeDedupSection(w *checkpointWriter) (uint64, error) {
	p.dedup.mu.Lock()
	defer p.dedup.mu.Unlock()
	pods := make([]string, 0, len(p.dedup.refs))
	tierSet := make(map[string]struct{})
	count := uint64(0)
	for pod, bucket := range p.dedup.refs {
		pods = append(pods, pod)
		count += uint64(len(bucket))
		for key := range bucket {
			tierSet[key.deviceTier] = struct{}{}
		}
	}
	tiers := make([]string, 0, len(tierSet))
	for tier := range tierSet {
		tiers = append(tiers, tier)
	}
	sort.Strings(pods)
	sort.Strings(tiers)
	podOrdinals, err := w.writeStringTable(pods)
	if err != nil {
		return 0, err
	}
	tierOrdinals, err := w.writeStringTable(tiers)
	if err != nil {
		return 0, err
	}
	if err := w.writeUint64(count); err != nil {
		return 0, err
	}
	for pod, bucket := range p.dedup.refs {
		for key, refs := range bucket {
			if err := w.writeUint32(podOrdinals[pod]); err != nil {
				return 0, err
			}
			if err := w.writeUint32(tierOrdinals[key.deviceTier]); err != nil {
				return 0, err
			}
			for _, value := range []int64{int64(key.groupIdx), int64(key.dataParallelRank)} {
				if err := w.writeInt64(value); err != nil {
					return 0, err
				}
			}
			if err := w.writeUint64(key.blockHash); err != nil {
				return 0, err
			}
			if refs <= 0 || uint64(refs) > math.MaxInt64 {
				return 0, errors.New("dedup reference count cannot be stored")
			}
			if err := w.writeUint64(uint64(refs)); err != nil {
				return 0, err
			}
		}
	}
	return count, nil
}

func readDedupSection(r io.Reader, section checkpointSection) (*eventDedupFilter, error) {
	reader := newCheckpointReader(r)
	pods, err := reader.readStringTable()
	if err != nil {
		return nil, fmt.Errorf("read dedup pod table: %w", err)
	}
	tiers, err := reader.readStringTable()
	if err != nil {
		return nil, fmt.Errorf("read dedup tier table: %w", err)
	}
	count, err := reader.readUint64()
	if err != nil {
		return nil, fmt.Errorf("read dedup count: %w", err)
	}
	if count != section.Count || count > reader.remaining()/40 {
		return nil, fmt.Errorf("invalid dedup count %d for a %d-byte section", count, section.Length)
	}
	if count > math.MaxInt {
		return nil, fmt.Errorf("dedup count %d is too large", count)
	}
	if len(pods) == 0 && count != 0 {
		return nil, errors.New("dedup records require a pod table")
	}
	if len(tiers) == 0 && count != 0 {
		return nil, errors.New("dedup records require a tier table")
	}
	result := newEventDedupFilter()
	for range count {
		podOrdinal, err := reader.readUint32()
		if err != nil {
			return nil, fmt.Errorf("read dedup pod ordinal: %w", err)
		}
		tierOrdinal, err := reader.readUint32()
		if err != nil {
			return nil, fmt.Errorf("read dedup tier ordinal: %w", err)
		}
		if uint64(podOrdinal) >= uint64(len(pods)) || uint64(tierOrdinal) >= uint64(len(tiers)) {
			return nil, errors.New("checkpoint contains an invalid dedup string ordinal")
		}
		group, err := reader.readInt64()
		if err != nil {
			return nil, fmt.Errorf("read dedup group: %w", err)
		}
		if group < math.MinInt || group > math.MaxInt {
			return nil, fmt.Errorf("invalid dedup group %d", group)
		}
		rank, err := reader.readInt64()
		if err != nil {
			return nil, fmt.Errorf("read dedup rank: %w", err)
		}
		if rank < math.MinInt || rank > math.MaxInt {
			return nil, fmt.Errorf("invalid dedup rank %d", rank)
		}
		hashValue, err := reader.readUint64()
		if err != nil {
			return nil, fmt.Errorf("read dedup block hash: %w", err)
		}
		refs, err := reader.readUint64()
		if err != nil {
			return nil, fmt.Errorf("read dedup reference count: %w", err)
		}
		if refs == 0 || refs > math.MaxInt {
			return nil, fmt.Errorf("invalid dedup reference count %d", refs)
		}
		pod := pods[podOrdinal]
		key := dedupKey{
			deviceTier: tiers[tierOrdinal], groupIdx: int(group),
			dataParallelRank: int(rank), blockHash: hashValue,
		}
		if result.refs[pod] == nil {
			result.refs[pod] = make(map[dedupKey]int)
		}
		if _, found := result.refs[pod][key]; found {
			return nil, errors.New("checkpoint contains duplicate dedup state")
		}
		result.refs[pod][key] = int(refs)
	}
	if err := cpRequireEOF(reader); err != nil {
		return nil, err
	}
	return result, nil
}

func writeGroupsSection(w *checkpointWriter, groups []kvblock.GroupCatalogSnapshotEntry) error {
	if len(groups) > checkpointMaxTableItems {
		return errors.New("checkpoint contains too many cache groups")
	}
	if err := w.writeUint64(uint64(len(groups))); err != nil {
		return err
	}
	for _, group := range groups {
		if group.PodIdentifier == "" || group.GroupID < 0 || group.Metadata.Kind == "" ||
			group.Metadata.BlockSize <= 0 ||
			(group.Metadata.SlidingWindowSize != nil && *group.Metadata.SlidingWindowSize < 0) {
			return errors.New("cache-group metadata cannot be stored")
		}
		if err := w.writeString(group.PodIdentifier); err != nil {
			return err
		}
		if err := w.writeString(group.Metadata.Kind); err != nil {
			return err
		}
		sliding := checkpointNoValue
		if group.Metadata.SlidingWindowSize != nil {
			sliding = int64(*group.Metadata.SlidingWindowSize)
		}
		for _, value := range []int64{int64(group.GroupID), int64(group.Metadata.BlockSize), sliding} {
			if err := w.writeInt64(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func readGroupsSection(r io.Reader, expected uint64) ([]kvblock.GroupCatalogSnapshotEntry, error) {
	reader := newCheckpointReader(r)
	count, err := reader.readUint64()
	if err != nil {
		return nil, fmt.Errorf("read cache-group count: %w", err)
	}
	if count != expected || count > checkpointMaxTableItems || count > reader.remaining()/32 || count > math.MaxInt {
		return nil, fmt.Errorf("invalid cache-group count %d", count)
	}
	groups := make([]kvblock.GroupCatalogSnapshotEntry, 0, min(int(count), 4096))
	seen := make(map[string]map[kvblock.GroupID]struct{})
	for range count {
		pod, err := reader.readString()
		if err != nil {
			return nil, fmt.Errorf("read cache-group pod: %w", err)
		}
		if pod == "" {
			return nil, errors.New("checkpoint contains an empty cache-group pod")
		}
		kind, err := reader.readString()
		if err != nil {
			return nil, fmt.Errorf("read cache-group kind: %w", err)
		}
		if kind == "" {
			return nil, errors.New("checkpoint contains an empty cache-group kind")
		}
		groupID, err := reader.readInt64()
		if err != nil {
			return nil, fmt.Errorf("read cache-group identifier: %w", err)
		}
		if groupID < 0 || groupID > math.MaxInt {
			return nil, fmt.Errorf("invalid cache-group identifier %d", groupID)
		}
		blockSize, err := reader.readInt64()
		if err != nil {
			return nil, fmt.Errorf("read cache-group block size: %w", err)
		}
		if blockSize <= 0 || blockSize > math.MaxInt {
			return nil, fmt.Errorf("invalid cache-group block size %d", blockSize)
		}
		sliding, err := reader.readInt64()
		if err != nil {
			return nil, fmt.Errorf("read cache-group sliding window: %w", err)
		}
		if sliding < checkpointNoValue || sliding > math.MaxInt {
			return nil, fmt.Errorf("invalid cache-group sliding window %d", sliding)
		}
		id := kvblock.GroupID(groupID)
		if seen[pod] == nil {
			seen[pod] = make(map[kvblock.GroupID]struct{})
		}
		if _, found := seen[pod][id]; found {
			return nil, errors.New("checkpoint contains duplicate cache-group state")
		}
		seen[pod][id] = struct{}{}
		metadata := kvblock.GroupMetadata{Kind: kind, BlockSize: int(blockSize)}
		if sliding != checkpointNoValue {
			value := int(sliding)
			metadata.SlidingWindowSize = &value
		}
		groups = append(groups, kvblock.GroupCatalogSnapshotEntry{PodIdentifier: pod, GroupID: id, Metadata: metadata})
	}
	return groups, cpRequireEOF(reader)
}

func writeSourcesSection(w *checkpointWriter, sources map[string]checkpointSource) (uint64, error) {
	if len(sources) > checkpointMaxTableItems {
		return 0, errors.New("checkpoint contains too many event sources")
	}
	ids := make([]string, 0, len(sources))
	for id := range sources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if err := w.writeUint64(uint64(len(ids))); err != nil {
		return 0, err
	}
	for _, id := range ids {
		source := sources[id]
		if id == "" || source.EventEndpoint == "" || source.ServingEndpoint == "" ||
			(source.DataParallelRank != nil && *source.DataParallelRank < 0) ||
			source.EventDigest == (checkpointDigest{}) {
			return 0, errors.New("event-source state cannot be stored")
		}
		for _, value := range []string{id, source.EventEndpoint, source.ServingEndpoint} {
			if err := w.writeString(value); err != nil {
				return 0, err
			}
		}
		rank := checkpointNoValue
		if source.DataParallelRank != nil {
			rank = int64(*source.DataParallelRank)
		}
		if err := w.writeInt64(rank); err != nil {
			return 0, err
		}
		if err := w.writeUint64(source.LastAppliedSequence); err != nil {
			return 0, err
		}
		if _, err := w.Write(source.EventDigest[:]); err != nil {
			return 0, err
		}
	}
	return uint64(len(ids)), nil
}

func readSourcesSection(r io.Reader, expected uint64) (map[string]checkpointSource, error) {
	reader := newCheckpointReader(r)
	count, err := reader.readUint64()
	if err != nil {
		return nil, fmt.Errorf("read event-source count: %w", err)
	}
	if count != expected || count > checkpointMaxTableItems || count > reader.remaining()/60 || count > math.MaxInt {
		return nil, fmt.Errorf("invalid event-source count %d", count)
	}
	sources := make(map[string]checkpointSource, min(int(count), 4096))
	for range count {
		id, err := reader.readString()
		if err != nil {
			return nil, fmt.Errorf("read event-source identifier: %w", err)
		}
		if id == "" {
			return nil, errors.New("checkpoint contains an empty event-source identifier")
		}
		eventEndpoint, err := reader.readString()
		if err != nil {
			return nil, fmt.Errorf("read event endpoint: %w", err)
		}
		if eventEndpoint == "" {
			return nil, errors.New("checkpoint contains an empty event endpoint")
		}
		servingEndpoint, err := reader.readString()
		if err != nil {
			return nil, fmt.Errorf("read serving endpoint: %w", err)
		}
		if servingEndpoint == "" {
			return nil, errors.New("checkpoint contains an empty serving endpoint")
		}
		rank, err := reader.readInt64()
		if err != nil {
			return nil, fmt.Errorf("read event-source rank: %w", err)
		}
		if rank < checkpointNoValue || rank > math.MaxInt {
			return nil, fmt.Errorf("invalid event-source rank %d", rank)
		}
		sequence, err := reader.readUint64()
		if err != nil {
			return nil, fmt.Errorf("read event-source sequence: %w", err)
		}
		var digest checkpointDigest
		if _, err := io.ReadFull(reader, digest[:]); err != nil {
			return nil, fmt.Errorf("read event-source digest: %w", err)
		}
		if digest == (checkpointDigest{}) {
			return nil, errors.New("checkpoint contains an empty payload digest")
		}
		if _, found := sources[id]; found {
			return nil, errors.New("checkpoint contains a duplicate event source")
		}
		source := checkpointSource{EventEndpoint: eventEndpoint, ServingEndpoint: servingEndpoint,
			LastAppliedSequence: sequence, EventDigest: digest}
		if rank != checkpointNoValue {
			value := int(rank)
			source.DataParallelRank = &value
		}
		sources[id] = source
	}
	return sources, cpRequireEOF(reader)
}

func encodeCheckpointHeader(header checkpointHeader) []byte {
	encoded := make([]byte, checkpointHeaderSize)
	copy(encoded, checkpointMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], checkpointSchemaVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], checkpointHeaderSize)
	binary.LittleEndian.PutUint64(encoded[16:24], uint64(header.CreatedAt.UnixNano()))
	copy(encoded[24:56], header.ConfigurationFingerprint[:])
	copy(encoded[56:88], header.BodyChecksum[:])
	binary.LittleEndian.PutUint32(encoded[88:92], checkpointSectionCount)
	position := 96
	for _, section := range header.Sections {
		binary.LittleEndian.PutUint64(encoded[position:position+8], section.Offset)
		binary.LittleEndian.PutUint64(encoded[position+8:position+16], section.Length)
		binary.LittleEndian.PutUint64(encoded[position+16:position+24], section.Count)
		position += 24
	}
	return encoded
}

func decodeCheckpointHeader(encoded []byte, fileSize uint64) (checkpointHeader, error) {
	if len(encoded) != checkpointHeaderSize || !bytes.Equal(encoded[:8], checkpointMagic[:]) {
		return checkpointHeader{}, errors.New("unsupported checkpoint format")
	}
	if version := binary.LittleEndian.Uint32(encoded[8:12]); version != checkpointSchemaVersion {
		return checkpointHeader{}, fmt.Errorf("unsupported checkpoint schema version %d", version)
	}
	if size := binary.LittleEndian.Uint32(encoded[12:16]); size != checkpointHeaderSize {
		return checkpointHeader{}, fmt.Errorf("invalid checkpoint header size %d", size)
	}
	header := checkpointHeader{CreatedAt: time.Unix(0, int64(binary.LittleEndian.Uint64(encoded[16:24]))).UTC()}
	copy(header.ConfigurationFingerprint[:], encoded[24:56])
	copy(header.BodyChecksum[:], encoded[56:88])
	if count := binary.LittleEndian.Uint32(encoded[88:92]); count != checkpointSectionCount {
		return checkpointHeader{}, fmt.Errorf("invalid checkpoint section count %d", count)
	}
	if binary.LittleEndian.Uint32(encoded[92:96]) != 0 {
		return checkpointHeader{}, errors.New("checkpoint contains unsupported header flags")
	}
	expectedOffset := uint64(checkpointHeaderSize)
	position := 96
	for i := range header.Sections {
		section := checkpointSection{Offset: binary.LittleEndian.Uint64(encoded[position : position+8]),
			Length: binary.LittleEndian.Uint64(encoded[position+8 : position+16]),
			Count:  binary.LittleEndian.Uint64(encoded[position+16 : position+24])}
		if section.Offset != expectedOffset || section.Offset > fileSize || section.Length > fileSize-section.Offset {
			return checkpointHeader{}, fmt.Errorf("invalid checkpoint section %d", i)
		}
		header.Sections[i] = section
		expectedOffset += section.Length
		position += 24
	}
	if expectedOffset != fileSize {
		return checkpointHeader{}, errors.New("checkpoint contains unreferenced data")
	}
	return header, nil
}

func decodeFingerprint(value string) ([sha256.Size]byte, error) {
	var fingerprint [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(fingerprint) {
		return fingerprint, errors.New("checkpoint configuration fingerprint must be a SHA-256 digest")
	}
	copy(fingerprint[:], decoded)
	return fingerprint, nil
}

func verifyCheckpointBody(file *os.File, size uint64, expected [sha256.Size]byte) error {
	digest := sha256.New()
	if _, err := io.Copy(digest, io.NewSectionReader(file, checkpointHeaderSize, int64(size-checkpointHeaderSize))); err != nil {
		return fmt.Errorf("checksum checkpoint: %w", err)
	}
	if !bytes.Equal(digest.Sum(nil), expected[:]) {
		return errors.New("checkpoint checksum mismatch")
	}
	return nil
}

func sectionReader(file *os.File, section checkpointSection) *io.SectionReader {
	return io.NewSectionReader(file, int64(section.Offset), int64(section.Length))
}

type checkpointWriter struct {
	writer  io.Writer
	offset  uint64
	limit   uint64
	scratch [8]byte
}

func (w *checkpointWriter) Write(data []byte) (int, error) {
	if w.offset > w.limit || uint64(len(data)) > w.limit-w.offset {
		return 0, errors.New("checkpoint exceeds the maximum file size")
	}
	written, err := w.writer.Write(data)
	w.offset += uint64(written)
	return written, err
}

func (w *checkpointWriter) section(start, count uint64) checkpointSection {
	return checkpointSection{Offset: start, Length: w.offset - start, Count: count}
}

func (w *checkpointWriter) writeString(value string) error {
	if len(value) > checkpointMaxString {
		return fmt.Errorf("checkpoint string is too long: %d bytes", len(value))
	}
	if err := w.writeUint32(uint32(len(value))); err != nil { // #nosec G115 -- checked above.
		return err
	}
	_, err := io.WriteString(w, value)
	return err
}

func (w *checkpointWriter) writeStringTable(values []string) (map[string]uint32, error) {
	if len(values) > checkpointMaxTableItems || uint64(len(values)) > math.MaxUint32 {
		return nil, errors.New("checkpoint string table is too large")
	}
	if err := w.writeUint32(uint32(len(values))); err != nil { // #nosec G115 -- checked above.
		return nil, err
	}
	ordinals := make(map[string]uint32, len(values))
	for ordinal, value := range values {
		if value == "" {
			return nil, errors.New("checkpoint string table contains an empty value")
		}
		if _, found := ordinals[value]; found {
			return nil, fmt.Errorf("checkpoint string table contains duplicate value %q", value)
		}
		if err := w.writeString(value); err != nil {
			return nil, err
		}
		ordinals[value] = uint32(ordinal) // #nosec G115 -- checked above.
	}
	return ordinals, nil
}

func (w *checkpointWriter) writeUint32(value uint32) error {
	binary.LittleEndian.PutUint32(w.scratch[:4], value)
	_, err := w.Write(w.scratch[:4])
	return err
}

func (w *checkpointWriter) writeUint64(value uint64) error {
	binary.LittleEndian.PutUint64(w.scratch[:8], value)
	_, err := w.Write(w.scratch[:8])
	return err
}

func (w *checkpointWriter) writeInt64(value int64) error {
	return w.writeUint64(uint64(value))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open checkpoint directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint directory: %w", err)
	}
	return nil
}

type checkpointReader struct {
	reader    *bufio.Reader
	bytesLeft uint64
	scratch   [8]byte
}

func newCheckpointReader(src io.Reader) *checkpointReader {
	bytesLeft := uint64(math.MaxUint64)
	if sized, ok := src.(interface{ Size() int64 }); ok && sized.Size() >= 0 {
		bytesLeft = uint64(sized.Size())
	}
	return &checkpointReader{reader: bufio.NewReaderSize(src, 1<<20), bytesLeft: bytesLeft}
}

func (r *checkpointReader) Read(dst []byte) (int, error) {
	read, err := r.reader.Read(dst)
	if uint64(read) > r.bytesLeft {
		r.bytesLeft = 0
	} else {
		r.bytesLeft -= uint64(read)
	}
	return read, err
}

func (r *checkpointReader) remaining() uint64 {
	return r.bytesLeft
}

func (r *checkpointReader) readString() (string, error) {
	length, err := r.readUint32()
	if err != nil {
		return "", err
	}
	if length > checkpointMaxString || uint64(length) > r.remaining() {
		return "", fmt.Errorf("invalid checkpoint string size %d", length)
	}
	value := make([]byte, length)
	if _, err := io.ReadFull(r, value); err != nil {
		return "", err
	}
	return string(value), nil
}

func (r *checkpointReader) readStringTable() ([]string, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if uint64(count) > checkpointMaxTableItems || uint64(count) > uint64(math.MaxInt) ||
		uint64(count) > r.remaining()/4 {
		return nil, fmt.Errorf("invalid checkpoint string-table size %d", count)
	}
	initialCapacity := min(int(count), 4096)
	values := make([]string, 0, initialCapacity)
	seen := make(map[string]struct{}, initialCapacity)
	for range count {
		value, err := r.readString()
		if err != nil {
			return nil, err
		}
		if value == "" {
			return nil, errors.New("checkpoint string table contains an empty value")
		}
		if _, found := seen[value]; found {
			return nil, fmt.Errorf("checkpoint string table contains duplicate value %q", value)
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values, nil
}

func (r *checkpointReader) readUint32() (uint32, error) {
	if _, err := io.ReadFull(r, r.scratch[:4]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(r.scratch[:4]), nil
}

func (r *checkpointReader) readUint64() (uint64, error) {
	if _, err := io.ReadFull(r, r.scratch[:8]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(r.scratch[:8]), nil
}

func (r *checkpointReader) readInt64() (int64, error) {
	value, err := r.readUint64()
	return int64(value), err
}

func cpRequireEOF(r io.Reader) error {
	var extra [1]byte
	count, err := r.Read(extra[:])
	if count != 0 || err == nil {
		return errors.New("checkpoint section contains trailing data")
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("read checkpoint section trailer: %w", err)
	}
	return nil
}

func (p *Pool) resumeSource(eventSourceID, eventEndpoint, servingEndpoint string, dataParallelRank *int) (checkpointSource, bool) {
	source, found := p.checkpointSource(eventSourceID)
	if !found || source.EventEndpoint != eventEndpoint || source.ServingEndpoint != servingEndpoint ||
		!equalOptionalInt(source.DataParallelRank, dataParallelRank) {
		return checkpointSource{}, false
	}
	return source, true
}

func (p *Pool) checkpointSource(eventSourceID string) (checkpointSource, bool) {
	p.appliedSourcesMu.RLock()
	source, found := p.appliedSources[eventSourceID]
	_, invalid := p.invalidSources[eventSourceID]
	p.appliedSourcesMu.RUnlock()
	if !found || invalid {
		return checkpointSource{}, false
	}
	source.DataParallelRank = cloneOptionalInt(source.DataParallelRank)
	return source, true
}

func (p *Pool) restoredSourcesAtServingEndpoint(
	servingEndpoint string,
	dataParallelRank *int,
) map[string]checkpointSource {
	p.appliedSourcesMu.RLock()
	defer p.appliedSourcesMu.RUnlock()
	sources := make(map[string]checkpointSource)
	for id, source := range p.appliedSources {
		if source.Generation != 0 || source.ServingEndpoint != servingEndpoint ||
			!equalOptionalInt(source.DataParallelRank, dataParallelRank) {
			continue
		}
		if _, invalid := p.invalidSources[id]; invalid {
			continue
		}
		source.DataParallelRank = cloneOptionalInt(source.DataParallelRank)
		sources[id] = source
	}
	return sources
}
