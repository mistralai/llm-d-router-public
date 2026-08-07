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

package routing_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
)

// TestParseDPScoringKey leans on the malformed cases. A key that is wrongly
// split yields a pod identifier that matches no endpoint, and the resulting
// miss is silent -- the endpoint scores zero and routing quietly falls back to
// load-only -- so anything that is not exactly "@dp<digits>" has to be left
// alone.
func TestParseDPScoringKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		wantPod  string
		wantRank int
	}{
		{"no suffix", "10.0.0.1:8000", "10.0.0.1:8000", routing.NoDataParallelRank},
		{"rank 0", "10.0.0.1:8000@dp0", "10.0.0.1:8000", 0},
		{"multi digit rank", "10.0.0.1:8000@dp12", "10.0.0.1:8000", 12},
		{"empty key", "", "", routing.NoDataParallelRank},
		{"ipv6 pod", "[2001:db8::1]:8000@dp2", "[2001:db8::1]:8000", 2},
		// Everything below must be treated as part of the pod identifier.
		{"non numeric", "10.0.0.1:8000@dpabc", "10.0.0.1:8000@dpabc", routing.NoDataParallelRank},
		{"negative", "10.0.0.1:8000@dp-3", "10.0.0.1:8000@dp-3", routing.NoDataParallelRank},
		{"empty digits", "10.0.0.1:8000@dp", "10.0.0.1:8000@dp", routing.NoDataParallelRank},
		{"leading plus", "10.0.0.1:8000@dp+3", "10.0.0.1:8000@dp+3", routing.NoDataParallelRank},
		{"leading zeros", "10.0.0.1:8000@dp007", "10.0.0.1:8000@dp007", routing.NoDataParallelRank},
		{"pod name contains @dp", "pod@dp-service:8080", "pod@dp-service:8080", routing.NoDataParallelRank},
		{"@dp in name plus real suffix", "pod@dp-service:8080@dp2", "pod@dp-service:8080", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod, rank := routing.ParseDPScoringKey(tt.key)
			assert.Equal(t, tt.wantPod, pod)
			assert.Equal(t, tt.wantRank, rank)
			assert.Equal(t, tt.wantPod, routing.StripDPRankSuffix(tt.key))
		})
	}
}

func TestBuildDPScoringKeyRoundTrips(t *testing.T) {
	for _, rank := range []int{0, 1, 7, 64} {
		key, err := routing.BuildDPScoringKey("10.0.0.1:8000", rank)
		require.NoError(t, err)

		pod, parsed := routing.ParseDPScoringKey(key)
		assert.Equal(t, "10.0.0.1:8000", pod)
		assert.Equal(t, rank, parsed)
	}

	// The sentinel means "no rank", so it must not be encoded.
	key, err := routing.BuildDPScoringKey("10.0.0.1:8000", routing.NoDataParallelRank)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:8000", key)

	// Any other negative is a caller bug: encoding it would produce a key that
	// ParseDPScoringKey refuses to read back.
	_, err = routing.BuildDPScoringKey("10.0.0.1:8000", -3)
	assert.Error(t, err)
}

func TestWinningRanksRoundTrip(t *testing.T) {
	ranks := map[string]int{"10.0.0.1:8000": 0, "10.0.0.2:8000": 3}

	encoded, err := routing.EncodeWinningRanks(ranks)
	require.NoError(t, err)

	decoded, err := routing.DecodeWinningRanks(encoded)
	require.NoError(t, err)
	assert.Equal(t, ranks, decoded)
}

func TestWinningRanksRejectsEmptyAndNegative(t *testing.T) {
	_, err := routing.EncodeWinningRanks(nil)
	assert.ErrorIs(t, err, routing.ErrEmptyWinningRanks)

	_, err = routing.EncodeWinningRanks(map[string]int{})
	assert.ErrorIs(t, err, routing.ErrEmptyWinningRanks)

	_, err = routing.EncodeWinningRanks(map[string]int{"10.0.0.1:8000": -1})
	assert.Error(t, err)

	_, err = routing.DecodeWinningRanks("")
	assert.ErrorIs(t, err, routing.ErrEmptyWinningRanks)

	_, err = routing.DecodeWinningRanks("{}")
	assert.ErrorIs(t, err, routing.ErrEmptyWinningRanks)

	_, err = routing.DecodeWinningRanks(`{"10.0.0.1:8000":-1}`)
	assert.Error(t, err)

	_, err = routing.DecodeWinningRanks("not json")
	assert.Error(t, err)
}
