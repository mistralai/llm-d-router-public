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
	"sort"
	"strings"

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

// ExportLatestClusterPrefixSnapshot uses the newest valid writer checkpoint.
// A failed candidate does not replace the current output.
func ExportLatestClusterPrefixSnapshot(directory, outputPath, configurationFingerprint string,
	meta kvblock.ClusterPrefixSnapshotMetadata,
) (string, kvblock.ClusterPrefixSnapshotResult, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return "", kvblock.ClusterPrefixSnapshotResult{}, fmt.Errorf("read checkpoint directory: %w", err)
	}
	candidates := make([]checkpointCandidate, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, checkpointFilePrefix) || !strings.HasSuffix(name, checkpointFileSuffix) {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
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
	var failures []error
	for _, candidate := range candidates {
		result, err := ExportClusterPrefixSnapshot(candidate.path, outputPath, configurationFingerprint, meta)
		if err == nil {
			return candidate.path, result, nil
		}
		failures = append(failures, fmt.Errorf("export %s: %w", candidate.path, err))
	}
	if len(failures) != 0 {
		return "", kvblock.ClusterPrefixSnapshotResult{}, errors.Join(failures...)
	}
	return "", kvblock.ClusterPrefixSnapshotResult{}, errors.New("checkpoint directory has no writer checkpoints")
}
