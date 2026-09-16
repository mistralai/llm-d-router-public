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

package kvblock_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// BenchmarkInMemoryIndexResidency measures the heap cost of a populated
// index at fixed occupancy, not per-operation latency:
//
//   - heap-bytes/key and heap-objects/key: resident bytes and heap objects
//     per request key after a forced GC. Heap objects are the GC scan
//     surface: every live object with pointers is walked per cycle.
//   - ingest-bytes/key: total bytes allocated while ingesting the keys,
//     amortized per key.
//   - forced-gc-ns: wall time of one runtime.GC over the populated heap.
//
// Run with -benchtime=1x so each sub-benchmark measures one population
// pass; without it the harness may repeat the closure, each pass being
// correct but slow. Compare across a layout change with go test -count
// and benchstat over the same metric names.
//
// Population mimics the event shape the kvevents subscriber produces:
// one pod entry per 64-block Add, repeated across the reporting pods.
func BenchmarkInMemoryIndexResidency(b *testing.B) {
	for _, numKeys := range []int{100_000, 1_000_000} {
		for _, numPods := range []int{1, 8} {
			b.Run(fmt.Sprintf("keys=%d/pods=%d", numKeys, numPods), func(b *testing.B) {
				// A fresh index per invocation keeps the measurement
				// correct if the harness calls the closure more than once.
				idx, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
					Size:         numKeys,
					PodCacheSize: 10,
				})
				if err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				keys := make([]kvblock.BlockHash, 0, 64)

				runtime.GC()
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)

				for p := 0; p < numPods; p++ {
					entry := []kvblock.PodEntry{{
						PodIdentifier: fmt.Sprintf("10.0.%d.%d:8000", p/256, p%256),
						DeviceTier:    "gpu",
					}}
					for base := 0; base < numKeys; base += 64 {
						keys = keys[:0]
						for j := base; j < numKeys && j < base+64; j++ {
							keys = append(keys, kvblock.BlockHash(uint64(j)+1))
						}
						if err := idx.Add(ctx, nil, keys, entry); err != nil {
							b.Fatal(err)
						}
					}
				}

				runtime.GC()
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)

				gcStart := time.Now()
				runtime.GC()
				forcedGC := time.Since(gcStart)

				keys = keys[:0]
				for j := 0; j < numKeys; j++ {
					keys = append(keys, kvblock.BlockHash(uint64(j)+1))
				}
				podSet := sets.New[string]()
				results, err := idx.Lookup(ctx, keys, podSet)
				if err != nil {
					b.Fatal(err)
				}
				if len(results) != numKeys {
					b.Fatalf("lookup found %d keys, want %d", len(results), numKeys)
				}

				keysPerUnit := float64(numKeys)
				b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/keysPerUnit, "heap-bytes/key")
				b.ReportMetric(float64(after.HeapObjects-before.HeapObjects)/keysPerUnit, "heap-objects/key")
				b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/keysPerUnit, "ingest-bytes/key")
				b.ReportMetric(float64(forcedGC.Nanoseconds()), "forced-gc-ns")
			})
		}
	}
}

// TestInMemoryIndexHeapProfile populates an index and writes a pprof heap
// profile of the live heap, for attributing resident bytes to the storage
// layout (go tool pprof -inuse_space). Skipped unless KVBLOCK_HEAP_PROFILE
// names the output file; KVBLOCK_PROFILE_KEYS and KVBLOCK_PROFILE_PODS set
// the occupancy (defaults 1M keys, 8 pods).
func TestInMemoryIndexHeapProfile(t *testing.T) {
	path := os.Getenv("KVBLOCK_HEAP_PROFILE")
	if path == "" {
		t.Skip("KVBLOCK_HEAP_PROFILE not set")
	}
	numKeys := envInt(t, "KVBLOCK_PROFILE_KEYS", 1_000_000)
	numPods := envInt(t, "KVBLOCK_PROFILE_PODS", 8)

	idx, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
		Size:         numKeys,
		PodCacheSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	entries := make([]kvblock.PodEntry, numPods)
	for p := range entries {
		entries[p] = kvblock.PodEntry{
			PodIdentifier: fmt.Sprintf("10.0.%d.%d:8000", p/256, p%256),
			DeviceTier:    "gpu",
		}
	}
	keys := make([]kvblock.BlockHash, 0, 64)
	for base := 0; base < numKeys; base += 64 {
		keys = keys[:0]
		for j := base; j < numKeys && j < base+64; j++ {
			keys = append(keys, kvblock.BlockHash(uint64(j)+1))
		}
		if err := idx.Add(ctx, nil, keys, entries); err != nil {
			t.Fatal(err)
		}
	}

	runtime.GC()
	runtime.GC()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}
	// idx must outlive the GC above, or the profile captures a collected heap.
	runtime.KeepAlive(idx)
	t.Logf("wrote heap profile of %d keys held by %d pods to %s", numKeys, numPods, path)
}

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("invalid %s %q", name, v)
	}
	return n
}
