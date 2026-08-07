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

package kvcache

import (
	"context"
	"fmt"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// KVScoringStrategy defines the strategy used to score pods for KV cache block reuse.
type KVScoringStrategy string

const (
	// LongestPrefixMatch Score by longest consecutive match from start.
	LongestPrefixMatch KVScoringStrategy = "LongestPrefix"
)

// KVBlockScorerConfig holds the configuration for the KVBlockScorer.
type KVBlockScorerConfig struct {
	ScoringStrategy KVScoringStrategy
	BackendConfigs  []*KVCacheBackendConfig `json:"backendConfigs"`
}

// DefaultKVBlockScorerConfig returns the default configuration for the KVBlockScorer.
func DefaultKVBlockScorerConfig() *KVBlockScorerConfig {
	return &KVBlockScorerConfig{
		ScoringStrategy: LongestPrefixMatch,
		BackendConfigs:  DefaultKVCacheBackendConfig(),
	}
}

// KVBlockScorer defines the interface for implementing a KV block scoring
// strategy.
type KVBlockScorer interface {
	// Strategy returns the scoring strategy type.
	Strategy() KVScoringStrategy
	// Score scores the blocks based on the scoring strategy.
	// It returns a map of pod names to their scores.
	Score(ctx context.Context, keys []kvblock.BlockHash,
		keyToPods map[kvblock.BlockHash][]kvblock.PodEntry) (map[string]float64, error)
}

// NewKVBlockScorer creates a new KVBlockScorer based on the provided strategy.
func NewKVBlockScorer(config *KVBlockScorerConfig) (KVBlockScorer, error) {
	switch config.ScoringStrategy {
	case LongestPrefixMatch:
		// Build weight map from list of BackendConfigs for efficient lookup
		weightMap := make(map[string]float64)
		for _, medium := range config.BackendConfigs {
			weightMap[medium.Name] = medium.Weight
		}

		return &LongestPrefixScorer{
			MediumWeights: weightMap,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported scoring strategy: %s", config.ScoringStrategy)
	}
}

// LongestPrefixScorer scores based on longest consecutive block matches count
// starting from block 0.
type LongestPrefixScorer struct {
	// mediumWeights maps medium/device tier names to their scoring weights
	MediumWeights map[string]float64
}

// Strategy returns the strategy type: LongestPrefixMatch.
func (s *LongestPrefixScorer) Strategy() KVScoringStrategy {
	return LongestPrefixMatch
}

// noDataParallelRank stands in for an entry with no data-parallel rank so the
// scoring identity can stay a comparable struct. Real ranks are non-negative.
const noDataParallelRank = -1

// podRank is the identity a consecutive prefix chain is tracked against.
//
// Data-parallel ranks sharing a pod hold independent KV caches, so the chain
// has to be walked per rank. If rank 0 holds blocks 0-9 and rank 1 holds blocks
// 10-19, no engine can serve the prefix from cache even though the pod
// collectively holds all twenty blocks; scoring the pod as a whole would claim
// a twenty-block match that neither rank can honour.
type podRank struct {
	podIdentifier    string
	dataParallelRank int
}

// entryPodRank returns the scoring identity of an index entry.
func entryPodRank(entry kvblock.PodEntry) podRank {
	rank := noDataParallelRank
	if entry.DataParallelRank != nil {
		rank = *entry.DataParallelRank
	}
	return podRank{podIdentifier: entry.PodIdentifier, dataParallelRank: rank}
}

// fillMaxWeights populates dst with the maximum weight per (pod, DP rank)
// across all device tiers for the given entries. The caller must clear dst
// before calling.
func fillMaxWeights(dst map[podRank]float64, entries []kvblock.PodEntry, mediumWeights map[string]float64) {
	for _, entry := range entries {
		weight := 1.0
		if mediumWeights != nil {
			if w, exists := mediumWeights[entry.DeviceTier]; exists {
				weight = w
			}
		}
		key := entryPodRank(entry)
		if cur, exists := dst[key]; !exists || weight > cur {
			dst[key] = weight
		}
	}
}

// Score implements the longest prefix scoring logic with weighted sum based on BackendConfig.
func (s *LongestPrefixScorer) Score(
	_ context.Context,
	keys []kvblock.BlockHash,
	keyToPods map[kvblock.BlockHash][]kvblock.PodEntry,
) (map[string]float64, error) {
	if len(keys) == 0 {
		return make(map[string]float64), nil
	}

	rankScores := make(map[podRank]float64)

	// Scratch map reused across iterations to avoid per-key allocation.
	curWeights := make(map[podRank]float64)

	// Build weight index for the first key in a single pass over entries.
	fillMaxWeights(curWeights, keyToPods[keys[0]], s.MediumWeights)

	// activeRanks tracks the (pod, DP rank) pairs still in the consecutive
	// prefix chain. Using a plain map and in-place deletion avoids allocating
	// new sets on every iteration.
	activeRanks := make(map[podRank]struct{}, len(curWeights))
	for pr, w := range curWeights {
		activeRanks[pr] = struct{}{}
		rankScores[pr] = w
	}

	for i := 1; i < len(keys); i++ {
		if len(activeRanks) == 0 {
			break
		}

		// Reuse scratch map: clear and refill for current key.
		clear(curWeights)
		fillMaxWeights(curWeights, keyToPods[keys[i]], s.MediumWeights)

		// In-place intersection: delete ranks that are not in the current key,
		// and accumulate scores for those that remain.
		for pr := range activeRanks {
			if w, exists := curWeights[pr]; exists {
				rankScores[pr] += w
			} else {
				delete(activeRanks, pr)
			}
		}
	}

	// Collapse to a pod-level score. Callers address endpoints by "ip:port"
	// (see the precise-prefix-cache producer), so the returned key must stay a
	// bare pod identifier: encoding the rank into it would make every lookup
	// miss and silently drop the deployment back to load-only routing. A pod's
	// score is its best rank's, since the request is served by one rank.
	podScores := make(map[string]float64, len(rankScores))
	for pr, score := range rankScores {
		if cur, exists := podScores[pr.podIdentifier]; !exists || score > cur {
			podScores[pr.podIdentifier] = score
		}
	}

	return podScores, nil
}
