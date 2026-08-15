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

package kvevents //nolint:testpackage // tests use unexported processEventBatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// ranksOf collects the data-parallel ranks recorded against a block, using -1
// to stand for an entry with no rank so both cases can be asserted uniformly.
func ranksOf(entries []kvblock.PodEntry) []int {
	ranks := make([]int, 0, len(entries))
	for i := range entries {
		if entries[i].DataParallelRank == nil {
			ranks = append(ranks, -1)
			continue
		}
		ranks = append(ranks, *entries[i].DataParallelRank)
	}
	return ranks
}

// TestDataParallelRanksAreIndexedIndependently covers the core invariant of DP
// awareness: ranks sharing a pod hold separate KV caches, so the same block
// announced by two ranks is two index entries, and a remove from one rank must
// not evict the other rank's copy.
//
// Before the rank was propagated, both stores collapsed onto a single pod-level
// entry sharing one dedup scope. The first remove would then be suppressed as a
// duplicate and the block would linger for a rank that had already dropped it.
func TestDataParallelRanksAreIndexedIndependently(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(context.Background())
	pool, idx, tp := newTestPool(t, 16)

	tokens := makeTokens(64)
	engineKeys := makeEngineKeys(4, 910)
	rank0, rank1 := 0, 1

	store := func(rank *int) {
		pool.processEventBatch(ctx, &EventBatch{
			Events: []GenericEvent{
				&BlockStoredEvent{BlockHashes: engineKeys, Tokens: tokens, ParentHash: 0},
			},
			DataParallelRank: rank,
		}, "pod-dp", "test-model")
	}

	store(&rank0)
	store(&rank1)

	canonicalKeys, err := tp.TokensToKVBlockKeys(kvblock.EmptyBlockHash, tokens, "test-model", nil)
	require.NoError(t, err)
	require.NotEmpty(t, canonicalKeys)
	probe := canonicalKeys[0]

	result, err := idx.Lookup(ctx, []kvblock.BlockHash{probe}, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{0, 1}, ranksOf(result[probe]),
		"each DP rank must be indexed as its own entry")

	// Rank 0 drops the block; rank 1 still holds it.
	pool.processEventBatch(ctx, &EventBatch{
		Events:           []GenericEvent{&BlockRemovedEvent{BlockHashes: engineKeys}},
		DataParallelRank: &rank0,
	}, "pod-dp", "test-model")

	result, err = idx.Lookup(ctx, []kvblock.BlockHash{probe}, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1}, ranksOf(result[probe]),
		"removing rank 0 must not evict the block rank 1 still holds")

	// Rank 1 drops it too; now the block is gone from the pod entirely.
	pool.processEventBatch(ctx, &EventBatch{
		Events:           []GenericEvent{&BlockRemovedEvent{BlockHashes: engineKeys}},
		DataParallelRank: &rank1,
	}, "pod-dp", "test-model")

	result, err = idx.Lookup(ctx, []kvblock.BlockHash{probe}, nil)
	require.NoError(t, err)
	assert.Empty(t, result[probe], "once every rank has removed it the block is gone")
}

func TestDataParallelAllBlocksClearedPreservesOtherRanks(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(context.Background())
	pool, idx, tp := newTestPool(t, 16)
	tokens := makeTokens(64)
	engineKeys := makeEngineKeys(4, 915)
	rank0, rank1 := 0, 1

	for _, rank := range []*int{&rank0, &rank1} {
		pool.processEventBatch(ctx, &EventBatch{
			Events: []GenericEvent{
				&BlockStoredEvent{BlockHashes: engineKeys, Tokens: tokens, ParentHash: 0},
			},
			DataParallelRank: rank,
		}, "pod-dp-clear", "test-model")
	}

	pool.processEventBatch(ctx, &EventBatch{
		Events:           []GenericEvent{&AllBlocksClearedEvent{}},
		DataParallelRank: &rank0,
	}, "pod-dp-clear", "test-model")

	canonicalKeys, err := tp.TokensToKVBlockKeys(kvblock.EmptyBlockHash, tokens, "test-model", nil)
	require.NoError(t, err)
	result, err := idx.Lookup(ctx, []kvblock.BlockHash{canonicalKeys[0]}, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1}, ranksOf(result[canonicalKeys[0]]),
		"a clear from rank 0 must preserve rank 1 entries")
}

// TestNonDataParallelEventsCarryNoRank guards the compatibility path: an engine
// that is not running data-parallel sends no rank, and its entries must stay
// rank-less so they encode exactly as they did before DP awareness.
func TestNonDataParallelEventsCarryNoRank(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(context.Background())
	pool, idx, tp := newTestPool(t, 16)

	tokens := makeTokens(64)
	engineKeys := makeEngineKeys(4, 920)

	pool.processEventBatch(ctx, &EventBatch{
		Events: []GenericEvent{
			&BlockStoredEvent{BlockHashes: engineKeys, Tokens: tokens, ParentHash: 0},
		},
	}, "pod-nodp", "test-model")

	canonicalKeys, err := tp.TokensToKVBlockKeys(kvblock.EmptyBlockHash, tokens, "test-model", nil)
	require.NoError(t, err)
	require.NotEmpty(t, canonicalKeys)
	probe := canonicalKeys[0]

	result, err := idx.Lookup(ctx, []kvblock.BlockHash{probe}, nil)
	require.NoError(t, err)
	require.Len(t, result[probe], 1)
	assert.Nil(t, result[probe][0].DataParallelRank,
		"a non-DP engine must produce an entry with no rank, not rank 0")

	// A single remove still fully evicts, exactly as before DP awareness.
	pool.processEventBatch(ctx, &EventBatch{
		Events: []GenericEvent{&BlockRemovedEvent{BlockHashes: engineKeys}},
	}, "pod-nodp", "test-model")

	result, err = idx.Lookup(ctx, []kvblock.BlockHash{probe}, nil)
	require.NoError(t, err)
	assert.Empty(t, result[probe])
}
