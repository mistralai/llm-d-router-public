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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGroupCatalogUnchangedLearnDoesNotWaitForReaders(t *testing.T) {
	window, equalWindow := 4096, 4096
	for _, tc := range []struct {
		name         string
		window, next *int
	}{
		{name: "no window"},
		{name: "equal window values", window: &window, next: &equalWindow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := NewGroupCatalog()
			meta := GroupMetadata{Kind: "kind-a", BlockSize: 128, SlidingWindowSize: tc.window}
			catalog.Learn("pod-a", 0, meta)
			meta.SlidingWindowSize = tc.next

			// An unrelated reader must not hold up learning unchanged metadata.
			catalog.mu.RLock()
			done := make(chan struct{})
			go func() {
				catalog.Learn("pod-a", 0, meta)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("unchanged metadata waited for an exclusive lock")
			}
			catalog.mu.RUnlock()
			<-done
		})
	}
}

func TestGroupCatalogMetadataChangesAndClear(t *testing.T) {
	catalog := NewGroupCatalog()
	zero, window, nextWindow := 0, 4096, 8192
	for _, meta := range []GroupMetadata{
		{},
		{Kind: "kind-a", BlockSize: 128},
		{Kind: "kind-b", BlockSize: 128},
		{Kind: "kind-b", BlockSize: 256},
		{Kind: "kind-b", BlockSize: 256, SlidingWindowSize: &zero},
		{Kind: "kind-b", BlockSize: 256, SlidingWindowSize: &window},
		{Kind: "kind-b", BlockSize: 256, SlidingWindowSize: &nextWindow},
		{Kind: "kind-b", BlockSize: 256},
	} {
		catalog.Learn("pod-a", 0, meta)
		got, found := catalog.Get("pod-a", 0)
		require.True(t, found)
		require.Equal(t, meta, got)
	}

	catalog.Clear("pod-a")
	_, found := catalog.Get("pod-a", 0)
	require.False(t, found)
	meta := GroupMetadata{Kind: "kind-b", BlockSize: 256}
	catalog.Learn("pod-a", 0, meta)
	got, found := catalog.Get("pod-a", 0)
	require.True(t, found)
	require.Equal(t, meta, got)
}

func TestGroupCatalogConcurrentLearnGetClear(t *testing.T) {
	catalog := NewGroupCatalog()
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			meta := GroupMetadata{Kind: strconv.Itoa(worker), BlockSize: worker}
			for range 1000 {
				catalog.Learn("pod-a", 0, meta)
				if got, found := catalog.Get("pod-a", 0); found && got.Kind != strconv.Itoa(got.BlockSize) {
					t.Errorf("inconsistent metadata: %+v", got)
				}
				catalog.Clear("pod-a")
			}
		})
	}
	workers.Wait()
}

// BenchmarkGroupCatalogRepeatedMetadata models event workers checking geometry
// and learning unchanged metadata for independent backends.
func BenchmarkGroupCatalogRepeatedMetadata(b *testing.B) {
	catalog := NewGroupCatalog()
	meta := GroupMetadata{Kind: "kind-a", BlockSize: 128}
	var nextPod atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		pod := strconv.FormatInt(nextPod.Add(1), 10)
		catalog.Learn(pod, 0, meta)
		for pb.Next() {
			catalog.Get(pod, 0)
			catalog.Learn(pod, 0, meta)
		}
	})
}
