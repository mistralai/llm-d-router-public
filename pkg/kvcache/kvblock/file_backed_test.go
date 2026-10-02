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
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileBackedIndexMergesSnapshotAndMutations(t *testing.T) {
	ctx := t.Context()
	source, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 2})
	require.NoError(t, err)
	baseEntry := PodEntry{PodIdentifier: "base", DeviceTier: "gpu"}
	require.NoError(t, source.Add(ctx, []BlockHash{11}, []BlockHash{101}, []PodEntry{baseEntry}))

	file, err := os.CreateTemp(t.TempDir(), "index-*.bin")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	require.NoError(t, source.WriteSnapshot(file))
	info, err := file.Stat()
	require.NoError(t, err)

	target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 2})
	require.NoError(t, err)
	require.NoError(t, target.RestoreSnapshotFile(file, 0, info.Size()))
	require.Zero(t, target.writerView().data.len)

	overlayEntry := PodEntry{PodIdentifier: "overlay", DeviceTier: "gpu"}
	require.NoError(t, target.Add(ctx, []BlockHash{12}, []BlockHash{101}, []PodEntry{overlayEntry}))
	hits, err := target.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []PodEntry{baseEntry, overlayEntry}, hits[101])
	require.Equal(t, BlockHash(101), mustRequestKey(t, target, 11))
	require.Equal(t, BlockHash(101), mustRequestKey(t, target, 12))

	require.NoError(t, target.Evict(ctx, 11, EngineKey, []PodEntry{baseEntry}))
	hits, err = target.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.Equal(t, []PodEntry{overlayEntry}, hits[101])

	require.NoError(t, target.Clear(ctx, "overlay"))
	hits, err = target.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.Empty(t, hits)

	require.NoError(t, target.Add(ctx, nil, []BlockHash{101}, []PodEntry{baseEntry}))
	hits, err = target.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.Equal(t, []PodEntry{baseEntry}, hits[101])
}

func TestFileBackedIndexEnforcesMergedPodCapacity(t *testing.T) {
	ctx := t.Context()
	source, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 2})
	require.NoError(t, err)
	first := PodEntry{PodIdentifier: "first", DeviceTier: "gpu"}
	second := PodEntry{PodIdentifier: "second", DeviceTier: "gpu"}
	third := PodEntry{PodIdentifier: "third", DeviceTier: "gpu"}
	require.NoError(t, source.Add(ctx, []BlockHash{11}, []BlockHash{101}, []PodEntry{first, second}))

	file, err := os.CreateTemp(t.TempDir(), "index-*.bin")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	require.NoError(t, source.WriteSnapshot(file))
	info, err := file.Stat()
	require.NoError(t, err)

	target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 2})
	require.NoError(t, err)
	require.NoError(t, target.RestoreSnapshotFile(file, 0, info.Size()))
	require.NoError(t, target.Add(ctx, nil, []BlockHash{101}, []PodEntry{third}))

	hits, err := target.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.Equal(t, []PodEntry{second, third}, hits[101])
}

func TestFileBackedIndexCompactsMergedView(t *testing.T) {
	ctx := t.Context()
	rankZero, rankOne := 0, 1
	first := PodEntry{PodIdentifier: "pod", DeviceTier: "gpu", DataParallelRank: &rankZero}
	second := PodEntry{PodIdentifier: "pod", DeviceTier: "gpu", DataParallelRank: &rankOne}
	third := PodEntry{PodIdentifier: "new", DeviceTier: "gpu"}
	source, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 3})
	require.NoError(t, err)
	require.NoError(t, source.Add(ctx, []BlockHash{11}, []BlockHash{101}, []PodEntry{first, second}))

	firstFile, err := os.CreateTemp(t.TempDir(), "base-*.bin")
	require.NoError(t, err)
	t.Cleanup(func() { _ = firstFile.Close() })
	require.NoError(t, source.WriteSnapshot(firstFile))
	firstInfo, err := firstFile.Stat()
	require.NoError(t, err)

	view, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 3})
	require.NoError(t, err)
	require.NoError(t, view.RestoreSnapshotFile(firstFile, 0, firstInfo.Size()))
	require.NoError(t, view.ClearRank(ctx, "pod", rankZero))
	require.NoError(t, view.Add(ctx, []BlockHash{12}, []BlockHash{101}, []PodEntry{third}))

	var compacted bytes.Buffer
	require.NoError(t, view.WriteSnapshot(&compacted))
	restored, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 3})
	require.NoError(t, err)
	require.NoError(t, restored.RestoreSnapshot(bytes.NewReader(compacted.Bytes())))
	hits, err := restored.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []PodEntry{second, third}, hits[101])
	require.Equal(t, BlockHash(101), mustRequestKey(t, restored, 11))
	require.Equal(t, BlockHash(101), mustRequestKey(t, restored, 12))
}

func TestFileBackedRestoreValidatesBeforeReplacingView(t *testing.T) {
	ctx := t.Context()
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 2})
	require.NoError(t, err)
	entry := PodEntry{PodIdentifier: "live", DeviceTier: "gpu"}
	require.NoError(t, index.Add(ctx, []BlockHash{11}, []BlockHash{101}, []PodEntry{entry}))

	file, err := os.CreateTemp(t.TempDir(), "invalid-*.bin")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	_, err = file.Write([]byte{1, 0, 0, 0})
	require.NoError(t, err)

	err = index.RestoreSnapshotFile(file, 0, 4)
	require.Error(t, err)
	hits, err := index.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.Equal(t, []PodEntry{entry}, hits[101])
}

func TestFileBackedIndexEnforcesCombinedRequestCapacity(t *testing.T) {
	ctx := t.Context()
	base, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	entry := PodEntry{PodIdentifier: "pod", DeviceTier: "gpu"}
	require.NoError(t, base.Add(ctx, nil, []BlockHash{101, 102}, []PodEntry{entry}))

	file, err := os.CreateTemp(t.TempDir(), "base-*.bin")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	require.NoError(t, base.WriteSnapshot(file))
	info, err := file.Stat()
	require.NoError(t, err)

	view, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	require.NoError(t, view.RestoreSnapshotFile(file, 0, info.Size()))
	require.NoError(t, view.Add(ctx, nil, []BlockHash{103}, []PodEntry{entry}))
	require.LessOrEqual(t, len(view.writerView().base.requestOffsets)+view.writerView().data.len, 2)

	var compacted bytes.Buffer
	require.NoError(t, view.WriteSnapshot(&compacted))
	restored, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	require.NoError(t, restored.RestoreSnapshot(bytes.NewReader(compacted.Bytes())))
}

func TestFileBackedOverlayEvictionDoesNotRevealOldRequestData(t *testing.T) {
	ctx := t.Context()
	base, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	oldEntry := PodEntry{PodIdentifier: "old", DeviceTier: "gpu"}
	require.NoError(t, base.Add(ctx, nil, []BlockHash{101, 102}, []PodEntry{oldEntry}))

	file, err := os.CreateTemp(t.TempDir(), "base-*.bin")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	require.NoError(t, base.WriteSnapshot(file))
	info, err := file.Stat()
	require.NoError(t, err)

	view, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	require.NoError(t, view.RestoreSnapshotFile(file, 0, info.Size()))
	newEntry := PodEntry{PodIdentifier: "new", DeviceTier: "gpu"}
	require.NoError(t, view.Add(ctx, nil, []BlockHash{101}, []PodEntry{newEntry}))
	require.NoError(t, view.Add(ctx, nil, []BlockHash{103, 104}, []PodEntry{newEntry}))

	hits, err := view.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.Empty(t, hits)
}

func TestFileBackedOverlayEvictionDoesNotRevealOldEngineMapping(t *testing.T) {
	ctx := t.Context()
	base, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	entry := PodEntry{PodIdentifier: "pod", DeviceTier: "gpu"}
	require.NoError(t, base.Add(ctx, []BlockHash{11, 12}, []BlockHash{101, 102}, []PodEntry{entry}))

	file, err := os.CreateTemp(t.TempDir(), "base-*.bin")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	require.NoError(t, base.WriteSnapshot(file))
	info, err := file.Stat()
	require.NoError(t, err)

	view, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	require.NoError(t, view.RestoreSnapshotFile(file, 0, info.Size()))
	require.NoError(t, view.Add(ctx, []BlockHash{11}, []BlockHash{101}, []PodEntry{entry}))
	require.NoError(t, view.Add(ctx, []BlockHash{13, 14}, []BlockHash{103, 104}, []PodEntry{entry}))

	_, err = view.GetRequestKey(ctx, 11)
	require.ErrorContains(t, err, "engine key not found")
	require.LessOrEqual(t, len(view.writerView().base.engineOffsets)+view.writerView().engineToRequestKeys.Len(), 2)
}

func TestFileBackedEvictionFailureKeepsBaseGeneration(t *testing.T) {
	ctx := t.Context()
	base, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 1, PodCacheSize: 2})
	require.NoError(t, err)
	first := PodEntry{PodIdentifier: "first", DeviceTier: "gpu"}
	second := PodEntry{PodIdentifier: "second", DeviceTier: "gpu"}
	require.NoError(t, base.Add(ctx, nil, []BlockHash{101}, []PodEntry{first, second}))

	file, err := os.CreateTemp(t.TempDir(), "base-*.bin")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	require.NoError(t, base.WriteSnapshot(file))
	info, err := file.Stat()
	require.NoError(t, err)

	view, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 1, PodCacheSize: 2})
	require.NoError(t, err)
	require.NoError(t, view.RestoreSnapshotFile(file, 0, info.Size()))
	view.writerView().data.nextChunk = uint32(len(view.writerView().data.refChunks))

	err = view.Evict(ctx, 101, RequestKey, []PodEntry{first})
	require.ErrorContains(t, err, "slab reference capacity exhausted")
	hits, err := view.Lookup(ctx, []BlockHash{101}, nil)
	require.NoError(t, err)
	require.Equal(t, []PodEntry{first, second}, hits[101])
}
