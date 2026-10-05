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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// ExportClusterPrefixSnapshot verifies a checkpoint and atomically writes its
// single-model cache directory. The checkpoint creation time controls freshness.
func ExportClusterPrefixSnapshot(checkpointPath, outputPath, configurationFingerprint string,
	meta kvblock.ClusterPrefixSnapshotMetadata,
) (kvblock.ClusterPrefixSnapshotResult, error) {
	if checkpointPath == "" || outputPath == "" {
		return kvblock.ClusterPrefixSnapshotResult{}, errors.New("checkpoint and output paths are required")
	}
	checkpointAbsolute, err := filepath.Abs(checkpointPath)
	if err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, err
	}
	outputAbsolute, err := filepath.Abs(outputPath)
	if err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, err
	}
	if checkpointAbsolute == outputAbsolute {
		return kvblock.ClusterPrefixSnapshotResult{}, errors.New("output path must differ from checkpoint path")
	}
	fingerprint, err := decodeFingerprint(configurationFingerprint)
	if err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, err
	}
	file, err := os.Open(checkpointPath)
	if err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("open checkpoint: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("stat checkpoint: %w", err)
	}
	if info.Size() < checkpointHeaderSize || uint64(info.Size()) > checkpointMaxFileSize {
		return kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("invalid checkpoint size %d", info.Size())
	}
	headerBytes := make([]byte, checkpointHeaderSize)
	if _, err := io.ReadFull(file, headerBytes); err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("read checkpoint header: %w", err)
	}
	header, err := decodeCheckpointHeader(headerBytes, uint64(info.Size()))
	if err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, err
	}
	if header.ConfigurationFingerprint != fingerprint {
		return kvblock.ClusterPrefixSnapshotResult{}, errors.New("checkpoint configuration does not match")
	}
	if err := verifyCheckpointBody(file, uint64(info.Size()), header.BodyChecksum); err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, err
	}
	meta.GeneratedAt = header.CreatedAt
	section := header.Sections[checkpointIndexSection]
	temporary, err := os.CreateTemp(filepath.Dir(outputPath), ".prefix-snapshot-*")
	if err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("create snapshot temporary file: %w", err)
	}
	defer os.Remove(temporary.Name())
	result, err := kvblock.WriteClusterPrefixSnapshot(temporary, file, int64(section.Offset),
		int64(section.Length), meta)
	if err != nil {
		temporary.Close()
		return kvblock.ClusterPrefixSnapshotResult{}, err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("sync snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(temporary.Name(), outputPath); err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("replace snapshot: %w", err)
	}
	if err := syncDirectory(filepath.Dir(outputPath)); err != nil {
		return kvblock.ClusterPrefixSnapshotResult{}, err
	}
	return result, nil
}
