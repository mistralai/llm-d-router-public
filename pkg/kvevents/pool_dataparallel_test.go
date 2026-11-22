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

package kvevents //nolint:testpackage // tests use unexported processEventBatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func TestDataParallelRanksAreIndexedIndependently(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(context.Background())
	pool, index, tokenProcessor := newTestPool(t, 16)
	podIdentifier := "10.0.0.1:8000"
	tokens := makeTokens(16)

	for rank := range 2 {
		pool.processEventBatch(ctx, &EventBatch{
			DataParallelRank: &rank,
			Events: []GenericEvent{
				&BlockStoredEvent{
					BlockHashes: []uint64{uint64(rank + 1)},
					Tokens:      tokens,
				},
			},
		}, podIdentifier, "test-model")
	}

	keys, err := tokenProcessor.TokensToKVBlockKeys(
		kvblock.EmptyBlockHash, tokens, "test-model", nil)
	require.NoError(t, err)
	require.Len(t, keys, 1)

	result, err := index.Lookup(ctx, keys, nil)
	require.NoError(t, err)
	require.Len(t, result[keys[0]], 2,
		"ranks behind one serving endpoint must retain independent cache entries")
}

func TestDataParallelRemovalPreservesSiblingRank(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(context.Background())
	pool, index, tokenProcessor := newTestPool(t, 16)
	podIdentifier := "10.0.0.1:8000"
	tokens := makeTokens(16)
	rank0, rank1 := 0, 1

	for _, rank := range []*int{&rank0, &rank1} {
		pool.processEventBatch(ctx, &EventBatch{
			DataParallelRank: rank,
			Events: []GenericEvent{
				&BlockStoredEvent{BlockHashes: []uint64{1}, Tokens: tokens},
			},
		}, podIdentifier, "test-model")
	}
	pool.processEventBatch(ctx, &EventBatch{
		DataParallelRank: &rank0,
		Events: []GenericEvent{
			&BlockRemovedEvent{BlockHashes: []uint64{1}},
		},
	}, podIdentifier, "test-model")

	keys, err := tokenProcessor.TokensToKVBlockKeys(
		kvblock.EmptyBlockHash, tokens, "test-model", nil)
	require.NoError(t, err)
	result, err := index.Lookup(ctx, keys, nil)
	require.NoError(t, err)
	require.Len(t, result[keys[0]], 1)
	require.NotNil(t, result[keys[0]][0].DataParallelRank)
	require.Equal(t, rank1, *result[keys[0]][0].DataParallelRank)
}

func TestDataParallelAllBlocksClearedPreservesSiblingRank(t *testing.T) {
	ctx := logging.NewTestLoggerIntoContext(context.Background())
	pool, index, tokenProcessor := newTestPool(t, 16)
	podIdentifier := "10.0.0.1:8000"
	tokens := makeTokens(16)
	rank0, rank1 := 0, 1

	for _, rank := range []*int{&rank0, &rank1} {
		pool.processEventBatch(ctx, &EventBatch{
			DataParallelRank: rank,
			Events: []GenericEvent{
				&BlockStoredEvent{BlockHashes: []uint64{1}, Tokens: tokens},
			},
		}, podIdentifier, "test-model")
	}
	pool.processEventBatch(ctx, &EventBatch{
		DataParallelRank: &rank0,
		Events:           []GenericEvent{&AllBlocksClearedEvent{}},
	}, podIdentifier, "test-model")

	keys, err := tokenProcessor.TokensToKVBlockKeys(
		kvblock.EmptyBlockHash, tokens, "test-model", nil)
	require.NoError(t, err)
	result, err := index.Lookup(ctx, keys, nil)
	require.NoError(t, err)
	require.Len(t, result[keys[0]], 1)
	require.NotNil(t, result[keys[0]][0].DataParallelRank)
	require.Equal(t, rank1, *result[keys[0]][0].DataParallelRank)
}
