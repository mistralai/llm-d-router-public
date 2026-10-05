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
	"slices"
	"time"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
)

const (
	clusterPrefixSnapshotVersion = 2
	clusterPrefixMaxStringLen    = 1<<16 - 1
)

// ClusterPrefixSnapshotMetadata identifies a single-model index export.
type ClusterPrefixSnapshotMetadata struct {
	Cluster         string
	Model           string
	BlockSizeTokens int
	HashSeed        string
	HashAlgorithm   string
	GeneratedAt     time.Time
}

// ClusterPrefixSnapshotResult reports the size of an export.
type ClusterPrefixSnapshotResult struct {
	Backends int
	Keys     int
}

// WriteClusterPrefixSnapshot writes confirmed request keys from one index checkpoint.
// The checkpoint must contain only the named model because its keys do not retain model names.
func WriteClusterPrefixSnapshot(dst io.Writer, file *os.File, offset, length int64,
	meta ClusterPrefixSnapshotMetadata,
) (ClusterPrefixSnapshotResult, error) {
	if meta.Cluster == "" || meta.Model == "" || meta.BlockSizeTokens <= 0 ||
		uint64(meta.BlockSizeTokens) > math.MaxUint32 || meta.GeneratedAt.IsZero() ||
		meta.GeneratedAt.UnixNano() <= 0 {
		return ClusterPrefixSnapshotResult{}, errors.New("invalid cluster prefix snapshot metadata")
	}
	algorithm := meta.HashAlgorithm
	if algorithm == "" {
		algorithm = HashAlgorithmCBORFNV
	}
	if algorithm != HashAlgorithmCBORFNV && algorithm != HashAlgorithmXXH64 {
		return ClusterPrefixSnapshotResult{}, fmt.Errorf("unsupported hash algorithm %q", algorithm)
	}
	for _, value := range []string{meta.Cluster, meta.Model, meta.HashSeed, algorithm} {
		if len(value) > clusterPrefixMaxStringLen {
			return ClusterPrefixSnapshotResult{}, errors.New("cluster prefix snapshot string is too long")
		}
	}
	snapshot, err := openMappedSnapshot(file, offset, length, math.MaxInt, math.MaxUint16)
	if err != nil {
		return ClusterPrefixSnapshotResult{}, fmt.Errorf("read index checkpoint: %w", err)
	}
	defer snapshot.close()

	byBackend := make(map[string][]BlockHash)
	refs := make([]CompactEntryRef, 0)
	for key := range snapshot.requestOffsets {
		refs, _ = snapshot.compactEntries(key, refs[:0])
		for _, ref := range refs {
			rank := routing.NoDataParallelRank
			if value, present := ref.DataParallelRank(); present {
				rank = value
			}
			backend, err := routing.BuildDPScoringKey(snapshot.pods[ref.PodOrdinal], rank)
			if err != nil {
				return ClusterPrefixSnapshotResult{}, err
			}
			byBackend[backend] = append(byBackend[backend], key)
		}
	}
	if uint64(len(byBackend)) > math.MaxUint32 {
		return ClusterPrefixSnapshotResult{}, errors.New("too many cache backends")
	}
	backends := make([]string, 0, len(byBackend))
	result := ClusterPrefixSnapshotResult{Backends: len(byBackend)}
	for backend, keys := range byBackend {
		if backend == "" || len(backend) > clusterPrefixMaxStringLen {
			return ClusterPrefixSnapshotResult{}, errors.New("invalid cache backend name")
		}
		slices.Sort(keys)
		keys = slices.Compact(keys)
		if uint64(len(keys)) > math.MaxUint32 {
			return ClusterPrefixSnapshotResult{}, fmt.Errorf("too many keys for backend %q", backend)
		}
		byBackend[backend] = keys
		result.Keys += len(keys)
		backends = append(backends, backend)
	}
	slices.Sort(backends)

	w := bufio.NewWriterSize(dst, 1<<20)
	var scratch [8]byte
	writeUint16 := func(value uint16) error {
		binary.LittleEndian.PutUint16(scratch[:2], value)
		_, err := w.Write(scratch[:2])
		return err
	}
	writeUint32 := func(value uint32) error {
		binary.LittleEndian.PutUint32(scratch[:4], value)
		_, err := w.Write(scratch[:4])
		return err
	}
	writeUint64 := func(value uint64) error {
		binary.LittleEndian.PutUint64(scratch[:], value)
		_, err := w.Write(scratch[:])
		return err
	}
	writeString := func(value string) error {
		if err := writeUint16(uint16(len(value))); err != nil { // #nosec G115 -- lengths are checked above.
			return err
		}
		_, err := io.WriteString(w, value)
		return err
	}
	if err := writeUint32(clusterPrefixSnapshotVersion); err != nil {
		return ClusterPrefixSnapshotResult{}, err
	}
	if err := writeUint32(uint32(meta.BlockSizeTokens)); err != nil { // #nosec G115 -- checked above.
		return ClusterPrefixSnapshotResult{}, err
	}
	if err := writeUint64(uint64(meta.GeneratedAt.UnixNano())); err != nil { // #nosec G115 -- checked above.
		return ClusterPrefixSnapshotResult{}, err
	}
	for _, value := range []string{meta.Cluster, meta.Model, meta.HashSeed, algorithm} {
		if err := writeString(value); err != nil {
			return ClusterPrefixSnapshotResult{}, err
		}
	}
	if err := writeUint32(uint32(len(backends))); err != nil { // #nosec G115 -- checked above.
		return ClusterPrefixSnapshotResult{}, err
	}
	for _, backend := range backends {
		if err := writeString(backend); err != nil {
			return ClusterPrefixSnapshotResult{}, err
		}
		keys := byBackend[backend]
		if err := writeUint32(uint32(len(keys))); err != nil { // #nosec G115 -- checked above.
			return ClusterPrefixSnapshotResult{}, err
		}
		for _, key := range keys {
			if err := writeUint64(uint64(key)); err != nil {
				return ClusterPrefixSnapshotResult{}, err
			}
		}
	}
	if err := w.Flush(); err != nil {
		return ClusterPrefixSnapshotResult{}, err
	}
	return result, nil
}
