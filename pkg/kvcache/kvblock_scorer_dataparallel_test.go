/*
Copyright 2025 The llm-d Authors.

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

package kvcache_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func dpEntry(pod string, rank int) kvblock.PodEntry {
	return kvblock.PodEntry{PodIdentifier: pod, DeviceTier: "gpu", DataParallelRank: &rank}
}

func scoreDP(t *testing.T, hitmap map[kvblock.BlockHash][]kvblock.PodEntry, keys []uint64) map[string]float64 {
	t.Helper()
	scorer := &kvcache.LongestPrefixScorer{MediumWeights: map[string]float64{"gpu": 1.0}}
	scored, err := scorer.Score(context.Background(), int64KeysToKVBlockKeys(keys), hitmap)
	require.NoError(t, err)
	return scored
}

// TestLongestPrefixScorerSplitAcrossRanks is the case that makes the prefix
// chain rank-aware: two DP ranks on one pod each hold half of the prefix, and
// neither can serve it from cache. Folding the ranks together would credit the
// pod with the union of both halves and route a request to a pod that then has
// to recompute most of the prompt.
func TestLongestPrefixScorerSplitAcrossRanks(t *testing.T) {
	// rank 0 holds the first two blocks, rank 1 the last two.
	scored := scoreDP(t, map[kvblock.BlockHash][]kvblock.PodEntry{
		2001: {dpEntry(podA, 0)},
		2002: {dpEntry(podA, 0)},
		2003: {dpEntry(podA, 1)},
		2004: {dpEntry(podA, 1)},
	}, []uint64{2001, 2002, 2003, 2004})

	// Rank 1 never matched the first block so it is not in the chain at all.
	assert.InDelta(t, 2.0, scored[podA+"@dp0"], 0.0001)
	assert.NotContains(t, scored, podA+"@dp1")

	podScores, winningRanks := kvcache.CollapseDPScoresToPods(scored)
	assert.InDelta(t, 2.0, podScores[podA], 0.0001,
		"a pod must not be credited with a prefix no single rank holds")
	assert.Equal(t, 0, winningRanks[podA])
}

// TestLongestPrefixScorerBestRankWins checks the collapse reports the strongest
// rank rather than whichever one map iteration happens to reach first.
func TestLongestPrefixScorerBestRankWins(t *testing.T) {
	// rank 0 holds only the first block; rank 1 holds all three.
	scored := scoreDP(t, map[kvblock.BlockHash][]kvblock.PodEntry{
		2101: {dpEntry(podA, 0), dpEntry(podA, 1)},
		2102: {dpEntry(podA, 1)},
		2103: {dpEntry(podA, 1)},
	}, []uint64{2101, 2102, 2103})

	assert.InDelta(t, 1.0, scored[podA+"@dp0"], 0.0001)
	assert.InDelta(t, 3.0, scored[podA+"@dp1"], 0.0001)

	podScores, winningRanks := kvcache.CollapseDPScoresToPods(scored)
	assert.InDelta(t, 3.0, podScores[podA], 0.0001, "the pod scores as its best rank")
	assert.Equal(t, 1, winningRanks[podA], "the best rank is the one to steer to")
}

// TestLongestPrefixScorerEmitsRankQualifiedKeys documents the scorer's side of
// the contract, and TestCollapseDPScoresToPodsYieldsBareKeys the consumer's.
// The pairing matters: the precise prefix cache producer looks scores up by a
// bare "ip:port" endpoint address, so a rank-qualified key reaching it would
// miss for every endpoint and silently degrade the deployment to load-only
// routing with nothing logged.
func TestLongestPrefixScorerEmitsRankQualifiedKeys(t *testing.T) {
	scored := scoreDP(t, map[kvblock.BlockHash][]kvblock.PodEntry{
		2201: {dpEntry(podA, 3)},
		2202: {dpEntry(podA, 3)},
	}, []uint64{2201, 2202})

	require.Contains(t, scored, podA+"@dp3")
	require.NotContains(t, scored, podA)
}

func TestCollapseDPScoresToPodsYieldsBareKeys(t *testing.T) {
	podScores, winningRanks := kvcache.CollapseDPScoresToPods(map[string]float64{
		podA + "@dp3": 2.0,
	})

	require.Contains(t, podScores, podA)
	for key := range podScores {
		assert.NotContains(t, key, "@dp", "a collapsed key must be a bare pod identifier")
	}
	assert.Equal(t, 3, winningRanks[podA])
}

// TestCollapseDPScoresToPodsPrefersUnrankedWinner covers a pod reporting both
// ranked and unranked entries, which happens while an engine is switching to
// data-parallel and stale unranked entries are still indexed. If the unranked
// entry wins there is no rank to steer to, so the pod must not be reported as
// having one.
func TestCollapseDPScoresToPodsPrefersUnrankedWinner(t *testing.T) {
	podScores, winningRanks := kvcache.CollapseDPScoresToPods(map[string]float64{
		podA:          5.0,
		podA + "@dp0": 1.0,
	})

	assert.InDelta(t, 5.0, podScores[podA], 0.0001)
	assert.NotContains(t, winningRanks, podA, "an unranked winner has no rank to pin")
}

// TestLongestPrefixScorerMixedRankedAndUnranked checks the chain itself keeps
// ranked and unranked entries apart rather than chaining one onto the other.
func TestLongestPrefixScorerMixedRankedAndUnranked(t *testing.T) {
	scored := scoreDP(t, map[kvblock.BlockHash][]kvblock.PodEntry{
		2301: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		2302: {dpEntry(podA, 0)},
	}, []uint64{2301, 2302})

	podScores, _ := kvcache.CollapseDPScoresToPods(scored)
	assert.InDelta(t, 1.0, podScores[podA], 0.0001,
		"an unranked entry must not extend a ranked entry's chain")
}
