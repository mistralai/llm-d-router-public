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
	"testing"

	. "github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/stretchr/testify/require"
)

func TestOtterIndexBehavior(t *testing.T) {
	testCommonIndexBehavior(t, func(t *testing.T) Index {
		t.Helper()
		index, err := NewOtterIndex(&OtterIndexConfig{Size: 1 << 20, PodCacheSize: 500})
		require.NoError(t, err)
		return index
	})
}

func TestOtterIndexSelectedWithDefaultInMemoryConfig(t *testing.T) {
	cfg := DefaultIndexConfig()
	cfg.OtterConfig = &OtterIndexConfig{Size: 100}
	index, err := NewIndex(t.Context(), cfg)
	require.NoError(t, err)
	require.IsType(t, &OtterIndex{}, index)
	require.Implements(t, (*KeyWalker)(nil), index)
	require.Implements(t, (*DataParallelRankIndex)(nil), index)
}
