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
	runtimemetrics "runtime/metrics"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

type churnProfile struct {
	name          string
	pods          int
	addKeys       int
	evictKeys     int
	clearInterval int
}

// BenchmarkInMemoryIndexChurn measures a full index under sustained KV event
// traffic while ordinary garbage collection remains enabled. The GLM shape
// uses its observed ratio of 37 admissions to 17 explicit evictions. The Kimi
// shape uses 398 admissions per message and one pod reset per 8,400 messages.
// An Add to the full index also exercises global LRU eviction.
//
// Use a fixed iteration count so setup is paid once and profiles see the same
// number of events. KVBLOCK_STRESS_KEYS controls resident size (default 1M):
//
//	go test -run=^$ -bench=InMemoryIndexChurn -benchtime=10000x -count=5 ./pkg/kvcache/kvblock/
func BenchmarkInMemoryIndexChurn(b *testing.B) {
	profiles := []churnProfile{
		{name: "full-lru", pods: 74, addKeys: 64},
		{name: "glm", pods: 74, addKeys: 37, evictKeys: 17},
		{name: "kimi-clear", pods: 140, addKeys: 398, clearInterval: 8_400},
	}
	residentKeys := churnResidentKeys(b)

	for _, profile := range profiles {
		b.Run(fmt.Sprintf("profile=%s/keys=%d/pods=%d", profile.name, residentKeys, profile.pods), func(b *testing.B) {
			benchmarkIndexChurn(b, residentKeys, profile)
		})
	}
}

func benchmarkIndexChurn(b *testing.B, residentKeys int, profile churnProfile) {
	b.Helper()
	runtime.GC()
	runtime.GC()
	var empty runtime.MemStats
	runtime.ReadMemStats(&empty)

	idx, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
		Size:         residentKeys,
		PodCacheSize: 10,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := logr.NewContext(context.Background(), logr.Discard())
	entries := make([][]kvblock.PodEntry, profile.pods)
	for pod := range entries {
		entries[pod] = []kvblock.PodEntry{{
			PodIdentifier: fmt.Sprintf("10.0.%d.%d:8000", pod/256, pod%256),
			DeviceTier:    "gpu",
		}}
	}

	const populateBatch = 64
	keys := make([]kvblock.BlockHash, populateBatch)
	recent := make([][]kvblock.BlockHash, profile.pods)
	validRecent := make([]bool, profile.pods)
	for pod := range recent {
		recent[pod] = make([]kvblock.BlockHash, profile.evictKeys)
	}
	for base := 0; base < residentKeys; base += populateBatch {
		batch := min(populateBatch, residentKeys-base)
		for i := range batch {
			keys[i] = kvblock.BlockHash(uint64(base+i) + 1)
		}
		pod := (base / populateBatch) % profile.pods
		if err := idx.Add(ctx, keys[:batch], keys[:batch], entries[pod]); err != nil {
			b.Fatal(err)
		}
		if batch >= profile.evictKeys {
			copy(recent[pod], keys[batch-profile.evictKeys:batch])
			validRecent[pod] = profile.evictKeys > 0
		}
	}

	addKeys := make([]kvblock.BlockHash, profile.addKeys)
	nextKey := uint64(residentKeys) + 1

	runtime.GC()
	runtime.GC()
	var populated runtime.MemStats
	runtime.ReadMemStats(&populated)
	gcStart := time.Now()
	runtime.GC()
	forcedGC := time.Since(gcStart)
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	gcCPUBefore := readGCCPUSeconds()
	start := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for event := 0; event < b.N; event++ {
		pod := event % profile.pods
		if validRecent[pod] {
			for _, key := range recent[pod] {
				if err := idx.Evict(ctx, key, kvblock.EngineKey, entries[pod]); err != nil {
					b.Fatal(err)
				}
			}
		}
		for i := range addKeys {
			addKeys[i] = kvblock.BlockHash(nextKey)
			nextKey++
		}
		if err := idx.Add(ctx, addKeys, addKeys, entries[pod]); err != nil {
			b.Fatal(err)
		}
		if profile.evictKeys > 0 {
			copy(recent[pod], addKeys[:profile.evictKeys])
			validRecent[pod] = true
		}
		if profile.clearInterval > 0 && (event+1)%profile.clearInterval == 0 {
			clearPod := ((event + 1) / profile.clearInterval) % profile.pods
			if err := idx.Clear(ctx, entries[clearPod][0].PodIdentifier); err != nil {
				b.Fatal(err)
			}
			validRecent[clearPod] = false
		}
	}
	b.StopTimer()
	elapsed := time.Since(start)
	gcCPUAfter := readGCCPUSeconds()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(idx)

	events := float64(b.N)
	b.ReportMetric(events/elapsed.Seconds(), "events/s")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/events, "runtime-B/event")
	b.ReportMetric(float64(after.PauseTotalNs-before.PauseTotalNs)/events, "gc-pause-ns/event")
	b.ReportMetric((gcCPUAfter-gcCPUBefore)*1e9/events, "gc-cpu-ns/event")
	b.ReportMetric(float64(after.NumGC-before.NumGC)*1000/events, "gc-cycles/kevent")
	b.ReportMetric(float64(int64(populated.HeapAlloc)-int64(empty.HeapAlloc))/float64(residentKeys), "resident-B/key")
	b.ReportMetric(float64(int64(populated.HeapObjects)-int64(empty.HeapObjects))/float64(residentKeys), "resident-objects/key")
	b.ReportMetric(float64(forcedGC.Nanoseconds()), "forced-gc-ns")
}

func churnResidentKeys(b *testing.B) int {
	b.Helper()
	const defaultResidentKeys = 1_000_000
	value := os.Getenv("KVBLOCK_STRESS_KEYS")
	if value == "" {
		return defaultResidentKeys
	}
	keys, err := strconv.Atoi(value)
	if err != nil || keys <= 0 {
		b.Fatalf("invalid KVBLOCK_STRESS_KEYS %q", value)
	}
	return keys
}

func readGCCPUSeconds() float64 {
	samples := []runtimemetrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	runtimemetrics.Read(samples)
	return samples[0].Value.Float64()
}
