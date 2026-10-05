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
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func readPrefixString(t *testing.T, r *bytes.Reader) string {
	t.Helper()
	var length uint16
	require.NoError(t, binary.Read(r, binary.LittleEndian, &length))
	value := make([]byte, length)
	_, err := io.ReadFull(r, value)
	require.NoError(t, err)
	return string(value)
}

func TestExportClusterPrefixSnapshot(t *testing.T) {
	pool, index, _ := newTestPool(t, 16)
	ctx := t.Context()
	require.NoError(t, index.Add(ctx, nil, []kvblock.BlockHash{101, 102},
		[]kvblock.PodEntry{{PodIdentifier: "pod-a", DeviceTier: "gpu"}}))
	rank := 2
	require.NoError(t, index.Add(ctx, nil, []kvblock.BlockHash{101},
		[]kvblock.PodEntry{{PodIdentifier: "pod-b", DeviceTier: "gpu", DataParallelRank: &rank}}))
	require.NoError(t, index.Add(ctx, nil, []kvblock.BlockHash{103},
		[]kvblock.PodEntry{{PodIdentifier: "pod-b", DeviceTier: "gpu", Speculative: true}}))
	checkpoint := filepath.Join(t.TempDir(), "checkpoint.bin")
	written, err := pool.WriteCheckpoint(checkpoint, testFingerprint)
	require.NoError(t, err)
	output := filepath.Join(t.TempDir(), "prefix.bin")
	meta := kvblock.ClusterPrefixSnapshotMetadata{
		Cluster: "c1", Model: "m1", BlockSizeTokens: 16,
		HashSeed: "test", HashAlgorithm: kvblock.HashAlgorithmCBORFNV,
	}
	result, err := ExportClusterPrefixSnapshot(checkpoint, output, testFingerprint, meta)
	require.NoError(t, err)
	require.Equal(t, kvblock.ClusterPrefixSnapshotResult{Backends: 2, Keys: 3}, result)
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	r := bytes.NewReader(data)
	var version, blockSize uint32
	var generated uint64
	require.NoError(t, binary.Read(r, binary.LittleEndian, &version))
	require.NoError(t, binary.Read(r, binary.LittleEndian, &blockSize))
	require.NoError(t, binary.Read(r, binary.LittleEndian, &generated))
	require.Equal(t, uint32(2), version)
	require.Equal(t, uint32(16), blockSize)
	require.Equal(t, written.CreatedAt.UnixNano(), int64(generated))
	for _, want := range []string{"c1", "m1", "test", kvblock.HashAlgorithmCBORFNV} {
		require.Equal(t, want, readPrefixString(t, r))
	}
	var backendCount uint32
	require.NoError(t, binary.Read(r, binary.LittleEndian, &backendCount))
	require.Equal(t, uint32(2), backendCount)
	for _, expected := range []struct {
		backend string
		keys    []uint64
	}{{"pod-a", []uint64{101, 102}}, {"pod-b@dp2", []uint64{101}}} {
		require.Equal(t, expected.backend, readPrefixString(t, r))
		var count uint32
		require.NoError(t, binary.Read(r, binary.LittleEndian, &count))
		require.Equal(t, uint32(len(expected.keys)), count)
		for _, want := range expected.keys {
			var key uint64
			require.NoError(t, binary.Read(r, binary.LittleEndian, &key))
			require.Equal(t, want, key)
		}
	}
	require.Zero(t, r.Len())
}

func TestExportClusterPrefixSnapshotRejectsCorruptCheckpoint(t *testing.T) {
	pool, index, _ := newTestPool(t, 16)
	require.NoError(t, index.Add(t.Context(), nil, []kvblock.BlockHash{101},
		[]kvblock.PodEntry{{PodIdentifier: "pod-a", DeviceTier: "gpu"}}))
	checkpoint := filepath.Join(t.TempDir(), "checkpoint.bin")
	_, err := pool.WriteCheckpoint(checkpoint, testFingerprint)
	require.NoError(t, err)
	data, err := os.ReadFile(checkpoint)
	require.NoError(t, err)
	data[len(data)-1] ^= 1
	require.NoError(t, os.WriteFile(checkpoint, data, 0o600))
	output := filepath.Join(t.TempDir(), "prefix.bin")
	require.NoError(t, os.WriteFile(output, []byte("keep"), 0o600))
	_, err = ExportClusterPrefixSnapshot(checkpoint, output, testFingerprint,
		kvblock.ClusterPrefixSnapshotMetadata{Cluster: "c1", Model: "m1", BlockSizeTokens: 16})
	require.ErrorContains(t, err, "checksum mismatch")
	contents, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), contents)
}

func TestExportLatestClusterPrefixSnapshotFallsBackToValidWriter(t *testing.T) {
	directory := t.TempDir()
	first, firstIndex, _ := newTestPool(t, 16)
	require.NoError(t, firstIndex.Add(t.Context(), nil, []kvblock.BlockHash{101},
		[]kvblock.PodEntry{{PodIdentifier: "pod-a", DeviceTier: "gpu"}}))
	firstResult, err := first.WriteCheckpointForWriter(directory, "writer-a", testFingerprint)
	require.NoError(t, err)
	second, secondIndex, _ := newTestPool(t, 16)
	require.NoError(t, secondIndex.Add(t.Context(), nil, []kvblock.BlockHash{102},
		[]kvblock.PodEntry{{PodIdentifier: "pod-b", DeviceTier: "gpu"}}))
	secondResult, err := second.WriteCheckpointForWriter(directory, "writer-b", testFingerprint)
	require.NoError(t, err)
	now := time.Now()
	require.NoError(t, os.Chtimes(firstResult.Path, now.Add(-time.Minute), now.Add(-time.Minute)))
	require.NoError(t, os.Chtimes(secondResult.Path, now, now))
	output := filepath.Join(t.TempDir(), "prefix.bin")
	meta := kvblock.ClusterPrefixSnapshotMetadata{Cluster: "c1", Model: "m1", BlockSizeTokens: 16}

	selected, result, err := ExportLatestClusterPrefixSnapshot(directory, output, testFingerprint, meta)
	require.NoError(t, err)
	require.Equal(t, secondResult.Path, selected)
	require.Equal(t, kvblock.ClusterPrefixSnapshotResult{Backends: 1, Keys: 1}, result)
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, uint64(102), binary.LittleEndian.Uint64(data[len(data)-8:]))

	checkpoint, err := os.ReadFile(secondResult.Path)
	require.NoError(t, err)
	checkpoint[len(checkpoint)-1] ^= 1
	require.NoError(t, os.WriteFile(secondResult.Path, checkpoint, 0o600))
	selected, result, err = ExportLatestClusterPrefixSnapshot(directory, output, testFingerprint, meta)
	require.NoError(t, err)
	require.Equal(t, firstResult.Path, selected)
	require.Equal(t, kvblock.ClusterPrefixSnapshotResult{Backends: 1, Keys: 1}, result)
	data, err = os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, uint64(101), binary.LittleEndian.Uint64(data[len(data)-8:]))

	checkpoint, err = os.ReadFile(firstResult.Path)
	require.NoError(t, err)
	checkpoint[len(checkpoint)-1] ^= 1
	require.NoError(t, os.WriteFile(firstResult.Path, checkpoint, 0o600))
	_, _, err = ExportLatestClusterPrefixSnapshot(directory, output, testFingerprint, meta)
	require.Error(t, err)
	unchanged, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, data, unchanged)
}
