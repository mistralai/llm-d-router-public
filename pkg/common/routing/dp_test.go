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

package routing_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
)

func TestDPScoringKeyRoundTrip(t *testing.T) {
	key, err := routing.BuildDPScoringKey("10.0.0.1:8000", 3)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:8000@dp3", key)

	pod, rank := routing.ParseDPScoringKey(key)
	assert.Equal(t, "10.0.0.1:8000", pod)
	assert.Equal(t, 3, rank)
}

func TestDPScoringKeyRejectsMalformedSuffix(t *testing.T) {
	for _, key := range []string{"pod@dp", "pod@dp-1", "pod@dp+1", "pod@dp01"} {
		pod, rank := routing.ParseDPScoringKey(key)
		assert.Equal(t, key, pod)
		assert.Equal(t, routing.NoDataParallelRank, rank)
	}
}
