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

	"github.com/llm-d/llm-d-router/pkg/common/routing"
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
	// Score scores the blocks based on the scoring strategy and returns a map
	// of scoring key to score. A key is a pod identifier, suffixed with
	// "@dp<rank>" when the entry came from a data-parallel engine, since ranks
	// sharing a pod cache independently and are scored independently. Callers
	// that address pods rather than ranks must run the result through
	// CollapseDPScoresToPods first.
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
	rank := routing.NoDataParallelRank
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

	// Emit one score per (pod, DP rank), with the rank encoded in the key so
	// callers that steer to a specific rank can recover it. Callers that
	// address whole pods must collapse the keys first -- see
	// CollapseDPScoresToPods.
	scores := make(map[string]float64, len(rankScores))
	for pr, score := range rankScores {
		key, err := routing.BuildDPScoringKey(pr.podIdentifier, pr.dataParallelRank)
		if err != nil {
			return nil, fmt.Errorf("failed to build scoring key for pod %q: %w", pr.podIdentifier, err)
		}
		scores[key] = score
	}

	return scores, nil
}

// CollapseDPScoresToPods reduces rank-qualified scores from KVBlockScorer.Score
// to one score per pod, keeping each pod's best rank and reporting which rank
// that was. Pods with no data-parallel rank are absent from winningRanks.
//
// Every consumer that addresses pods rather than ranks must call this. A raw
// "<pod>@dp<rank>" key will not match a plain "ip:port" endpoint address, and
// the lookup miss is silent: the endpoint simply scores zero and the request
// falls back to load-only routing with nothing logged.
func CollapseDPScoresToPods(scores map[string]float64) (podScores map[string]float64, winningRanks map[string]int) {
	podScores = make(map[string]float64, len(scores))
	winningRanks = make(map[string]int)
	selectedRanks := make(map[string]int)

	for key, score := range scores {
		podIdentifier, rank := routing.ParseDPScoringKey(key)
		if cur, exists := podScores[podIdentifier]; exists {
			selectedRank := selectedRanks[podIdentifier]
			if score < cur || (score == cur && rank >= selectedRank) {
				continue
			}
		}
		podScores[podIdentifier] = score
		selectedRanks[podIdentifier] = rank
		if rank == routing.NoDataParallelRank {
			delete(winningRanks, podIdentifier)
			continue
		}
		winningRanks[podIdentifier] = rank
	}

	return podScores, winningRanks
}
