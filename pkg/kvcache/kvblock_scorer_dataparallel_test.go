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

// TestLongestPrefixScorerSplitAcrossRanks is the case that makes the prefix
// chain rank-aware: two DP ranks on one pod each hold half of the prefix, and
// neither can serve it from cache. Scoring the pod as a whole would credit it
// with the union of both halves and route a request to a pod that then has to
// recompute most of the prompt.
func TestLongestPrefixScorerSplitAcrossRanks(t *testing.T) {
	scorer := &kvcache.LongestPrefixScorer{MediumWeights: map[string]float64{"gpu": 1.0}}
	blockKeys := int64KeysToKVBlockKeys([]uint64{2001, 2002, 2003, 2004})

	// rank 0 holds the first two blocks, rank 1 the last two.
	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		2001: {dpEntry(podA, 0)},
		2002: {dpEntry(podA, 0)},
		2003: {dpEntry(podA, 1)},
		2004: {dpEntry(podA, 1)},
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	require.NoError(t, err)

	// The chain breaks at block 3 for rank 0, and rank 1 never matched block 1
	// so it is not in the chain at all. Best rank scores 2, not 4.
	assert.InDelta(t, 2.0, scored[podA], 0.0001,
		"a pod must not be credited with a prefix no single rank holds")
}

// TestLongestPrefixScorerBestRankWins checks the collapse picks the strongest
// rank rather than the first or last one encountered.
func TestLongestPrefixScorerBestRankWins(t *testing.T) {
	scorer := &kvcache.LongestPrefixScorer{MediumWeights: map[string]float64{"gpu": 1.0}}
	blockKeys := int64KeysToKVBlockKeys([]uint64{2101, 2102, 2103})

	// rank 0 holds only the first block; rank 1 holds all three.
	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		2101: {dpEntry(podA, 0), dpEntry(podA, 1)},
		2102: {dpEntry(podA, 1)},
		2103: {dpEntry(podA, 1)},
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	require.NoError(t, err)
	assert.InDelta(t, 3.0, scored[podA], 0.0001, "the pod scores as its best rank")
}

// TestLongestPrefixScorerKeysStayPodLevel guards the contract the precise
// prefix cache producer depends on: it looks scores up by a bare "ip:port"
// endpoint address, so a rank-qualified key would miss for every endpoint and
// silently degrade the deployment to load-only routing.
func TestLongestPrefixScorerKeysStayPodLevel(t *testing.T) {
	scorer := &kvcache.LongestPrefixScorer{MediumWeights: map[string]float64{"gpu": 1.0}}
	blockKeys := int64KeysToKVBlockKeys([]uint64{2201, 2202})

	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		2201: {dpEntry(podA, 3)},
		2202: {dpEntry(podA, 3)},
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	require.NoError(t, err)

	require.Contains(t, scored, podA, "score must be keyed by the bare pod identifier")
	for key := range scored {
		assert.NotContains(t, key, "@dp", "the DP rank must not leak into the scoring key")
	}
}

// TestLongestPrefixScorerMixedRankedAndUnranked covers a pod that reports both
// ranked and unranked entries, which happens while an engine is being switched
// to data-parallel and stale unranked entries are still indexed. The two are
// distinct identities and must not be chained together.
func TestLongestPrefixScorerMixedRankedAndUnranked(t *testing.T) {
	scorer := &kvcache.LongestPrefixScorer{MediumWeights: map[string]float64{"gpu": 1.0}}
	blockKeys := int64KeysToKVBlockKeys([]uint64{2301, 2302})

	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		2301: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		2302: {dpEntry(podA, 0)},
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, scored[podA], 0.0001,
		"an unranked entry must not extend a ranked entry's chain")
}
