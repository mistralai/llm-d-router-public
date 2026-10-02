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
	"encoding/binary"
	"io"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInMemoryIndexCheckpointRoundTrip(t *testing.T) {
	ctx := t.Context()
	source, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	rank := 2
	confirmed := PodEntry{
		PodIdentifier:    "10.0.0.1:8000",
		DeviceTier:       "gpu",
		HasGroup:         true,
		GroupIdx:         3,
		DataParallelRank: &rank,
	}
	require.NoError(t, source.Add(ctx,
		[]BlockHash{11, 12}, []BlockHash{101, 102}, []PodEntry{confirmed}))
	require.NoError(t, source.Add(ctx, nil, []BlockHash{103}, []PodEntry{{
		PodIdentifier: "10.0.0.2:8000", DeviceTier: "gpu", Speculative: true,
	}}))

	var checkpoint bytes.Buffer
	err = source.WriteSnapshot(&checkpoint)
	require.NoError(t, err)

	target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	require.NoError(t, target.RestoreSnapshot(bytes.NewReader(checkpoint.Bytes())))

	hits, err := target.Lookup(ctx, []BlockHash{101, 102, 103}, nil)
	require.NoError(t, err)
	require.Equal(t, []PodEntry{confirmed}, hits[101])
	require.Equal(t, []PodEntry{confirmed}, hits[102])
	require.Empty(t, hits[103])
	require.Equal(t, BlockHash(102), mustRequestKey(t, target, 12))
}

func TestInMemoryIndexRestoreValidatesBeforeReplacingState(t *testing.T) {
	ctx := t.Context()
	target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 2, PodCacheSize: 1})
	require.NoError(t, err)
	require.NoError(t, target.Add(ctx,
		[]BlockHash{22}, []BlockHash{202}, []PodEntry{{PodIdentifier: "live", DeviceTier: "gpu"}}))

	err = target.RestoreSnapshot(bytes.NewReader([]byte{1, 0, 0, 0}))
	require.Error(t, err)

	hits, err := target.Lookup(ctx, []BlockHash{202}, nil)
	require.NoError(t, err)
	require.Equal(t, "live", hits[202][0].PodIdentifier)
}

func TestInMemoryIndexCheckpointExcludesSpeculativeMappings(t *testing.T) {
	ctx := t.Context()
	source, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	require.NoError(t, source.Add(ctx, []BlockHash{11}, []BlockHash{101}, []PodEntry{{
		PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu", Speculative: true,
	}}))

	var checkpoint bytes.Buffer
	require.NoError(t, source.WriteSnapshot(&checkpoint))

	target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	require.NoError(t, target.RestoreSnapshot(bytes.NewReader(checkpoint.Bytes())))
	_, err = target.GetRequestKey(ctx, 11)
	require.Error(t, err)
}

func TestInMemoryIndexCheckpointDoesNotBlockLookups(t *testing.T) {
	ctx := t.Context()
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	require.NoError(t, index.Add(ctx, []BlockHash{11}, []BlockHash{101}, []PodEntry{{
		PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu",
	}}))
	writer := newBlockingWriter()
	defer writer.unblock()
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- index.WriteSnapshot(writer)
	}()
	<-writer.started

	lookupDone := make(chan error, 1)
	go func() {
		_, lookupErr := index.Lookup(ctx, []BlockHash{101}, nil)
		lookupDone <- lookupErr
	}()
	select {
	case lookupErr := <-lookupDone:
		require.NoError(t, lookupErr)
	case <-time.After(time.Second):
		t.Fatal("checkpoint write blocked an index lookup")
	}
	writer.unblock()
	require.NoError(t, <-writeDone)
}

func TestInMemoryIndexRestoreRejectsUnknownReferenceFlags(t *testing.T) {
	var checkpoint bytes.Buffer
	require.NoError(t, writeUint32(&checkpoint, indexSnapshotVersion))
	require.NoError(t, writeUint32(&checkpoint, 1))
	require.NoError(t, writeString(&checkpoint, "pod"))
	require.NoError(t, writeUint32(&checkpoint, 1))
	require.NoError(t, writeString(&checkpoint, "gpu"))
	require.NoError(t, writeUint64(&checkpoint, 1))
	require.NoError(t, writeUint64(&checkpoint, 101))
	require.NoError(t, writeUint16(&checkpoint, 1))
	for _, value := range []uint32{0, 0, 1 << 8, math.MaxUint32, math.MaxUint32} {
		require.NoError(t, writeUint32(&checkpoint, value))
	}
	require.NoError(t, writeUint64(&checkpoint, 0))

	target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	err = target.RestoreSnapshot(bytes.NewReader(checkpoint.Bytes()))
	require.ErrorContains(t, err, "invalid cache location")
}

func TestInMemoryIndexRestoreRejectsOversizedStringTable(t *testing.T) {
	var checkpoint bytes.Buffer
	require.NoError(t, binary.Write(&checkpoint, binary.LittleEndian, uint32(indexSnapshotVersion)))
	require.NoError(t, binary.Write(&checkpoint, binary.LittleEndian, uint32(maxInternedPods)))

	target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 8, PodCacheSize: 4})
	require.NoError(t, err)
	err = target.RestoreSnapshot(bytes.NewReader(checkpoint.Bytes()))
	require.ErrorContains(t, err, "invalid string-table size")
}

func TestCheckpointCapabilitySurvivesIndexWrappers(t *testing.T) {
	index, err := NewInMemoryIndex(DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	for name, wrapped := range map[string]Index{
		"instrumented": NewInstrumentedIndex(index),
		"traced":       NewTracedIndex(index),
	} {
		t.Run(name, func(t *testing.T) {
			_, isSnapshotter := wrapped.(Snapshotter)
			require.True(t, isSnapshotter)
			_, isFileSnapshotter := wrapped.(FileSnapshotter)
			require.True(t, isFileSnapshotter)
			_, isCompactWalker := wrapped.(CompactKeyWalker)
			require.True(t, isCompactWalker)
		})
	}
}

func TestSnapshotOnlyCapabilitySurvivesIndexWrappers(t *testing.T) {
	index, err := NewInMemoryIndex(DefaultInMemoryIndexConfig())
	require.NoError(t, err)
	snapshotOnly := &snapshotOnlyIndex{Index: index, snapshotter: index}
	for name, wrapped := range map[string]Index{
		"instrumented": NewInstrumentedIndex(snapshotOnly),
		"traced":       NewTracedIndex(snapshotOnly),
	} {
		t.Run(name, func(t *testing.T) {
			_, isSnapshotter := wrapped.(Snapshotter)
			require.True(t, isSnapshotter)
			_, isFileSnapshotter := wrapped.(FileSnapshotter)
			require.False(t, isFileSnapshotter)
		})
	}
}

type snapshotOnlyIndex struct {
	Index
	snapshotter Snapshotter
}

func (i *snapshotOnlyIndex) WriteSnapshot(dst io.Writer) error {
	return i.snapshotter.WriteSnapshot(dst)
}

func (i *snapshotOnlyIndex) RestoreSnapshot(src io.Reader) error {
	return i.snapshotter.RestoreSnapshot(src)
}

func mustRequestKey(t *testing.T, index *InMemoryIndex, engineKey BlockHash) BlockHash {
	t.Helper()
	requestKey, err := index.GetRequestKey(t.Context(), engineKey)
	require.NoError(t, err)
	return requestKey
}

func BenchmarkInMemoryIndexWriteSnapshot(b *testing.B) {
	index := benchmarkCheckpointIndex(b)
	var size countingWriter
	require.NoError(b, index.WriteSnapshot(&size))
	b.ReportMetric(float64(size.written)/100_000, "bytes/entry")
	b.SetBytes(size.written)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		require.NoError(b, index.WriteSnapshot(io.Discard))
	}
}

func BenchmarkInMemoryIndexWriteFileBackedSnapshot(b *testing.B) {
	source := benchmarkCheckpointIndex(b)
	file, err := os.CreateTemp(b.TempDir(), "index-*.bin")
	require.NoError(b, err)
	defer file.Close()
	require.NoError(b, source.WriteSnapshot(file))
	info, err := file.Stat()
	require.NoError(b, err)
	target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 100_000, PodCacheSize: 4})
	require.NoError(b, err)
	require.NoError(b, target.RestoreSnapshotFile(file, 0, info.Size()))
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		require.NoError(b, target.WriteSnapshot(io.Discard))
	}
}

func BenchmarkInMemoryIndexRestoreSnapshot(b *testing.B) {
	index := benchmarkCheckpointIndex(b)
	var checkpoint bytes.Buffer
	require.NoError(b, index.WriteSnapshot(&checkpoint))
	b.ReportMetric(float64(checkpoint.Len())/100_000, "bytes/entry")
	b.SetBytes(int64(checkpoint.Len()))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 100_000, PodCacheSize: 4})
		require.NoError(b, err)
		require.NoError(b, target.RestoreSnapshot(bytes.NewReader(checkpoint.Bytes())))
	}
}

func BenchmarkInMemoryIndexRestoreSnapshotFile(b *testing.B) {
	index := benchmarkCheckpointIndex(b)
	file, err := os.CreateTemp(b.TempDir(), "index-*.bin")
	require.NoError(b, err)
	defer file.Close()
	require.NoError(b, index.WriteSnapshot(file))
	info, err := file.Stat()
	require.NoError(b, err)
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		target, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: 100_000, PodCacheSize: 4})
		require.NoError(b, err)
		require.NoError(b, target.RestoreSnapshotFile(file, 0, info.Size()))
		target.writerView().base.close()
	}
}

func benchmarkCheckpointIndex(b *testing.B) *InMemoryIndex {
	const size = 100_000
	b.Helper()
	index, err := NewInMemoryIndex(&InMemoryIndexConfig{Size: size, PodCacheSize: 4})
	require.NoError(b, err)
	entry := PodEntry{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu"}
	const batchSize = 1_000
	for start := 0; start < size; start += batchSize {
		count := min(batchSize, size-start)
		engineKeys := make([]BlockHash, count)
		requestKeys := make([]BlockHash, count)
		for i := range count {
			engineKeys[i] = BlockHash(start + i + 1)
			requestKeys[i] = BlockHash(size + start + i + 1)
		}
		require.NoError(b, index.Add(b.Context(), engineKeys, requestKeys, []PodEntry{entry}))
	}
	return index
}

type countingWriter struct {
	written int64
}

type blockingWriter struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingWriter) Write(value []byte) (int, error) {
	w.startedOnce.Do(func() { close(w.started) })
	<-w.release
	return len(value), nil
}

func (w *blockingWriter) unblock() {
	w.releaseOnce.Do(func() { close(w.release) })
}

func (w *countingWriter) Write(value []byte) (int, error) {
	w.written += int64(len(value))
	return len(value), nil
}
