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

package dataparallel

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	preciseprefixcacheconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/preciseprefixcache/constants"
)

const dpTestProfile = "default"

// dpResultForFirstEndpoint builds a scheduling result whose selected endpoint
// is 10.0.0.1:8000, matching newMockProfileRunResult's numbering.
func dpResultForFirstEndpoint() *scheduling.SchedulingResult {
	return &scheduling.SchedulingResult{
		PrimaryProfileName: dpTestProfile,
		ProfileResults: map[string]*scheduling.ProfileRunResult{
			dpTestProfile: newMockProfileRunResult(DefaultTestPodPort, "pod-1"),
		},
	}
}

func runDPRankHandler(t *testing.T, headers map[string]string, ranks map[string]int,
	result *scheduling.SchedulingResult,
) map[string]string {
	t.Helper()
	request := &scheduling.InferenceRequest{Headers: headers}
	if ranks != nil {
		request.PutAttribute(preciseprefixcacheconstants.WinningRanksDataKey, ranks)
	}
	require.NoError(t, NewDPRankHeaderHandler().PreRequest(context.Background(), request, result))
	return request.Headers
}

func TestDPRankHeaderHandlerPinsSelectedEndpointRank(t *testing.T) {
	ranks := map[string]int{
		"10.0.0.1:8000": 2,
		"10.0.0.9:8000": 5, // a pod that was not selected
	}

	headers := runDPRankHandler(t, map[string]string{}, ranks, dpResultForFirstEndpoint())

	assert.Equal(t, "2", headers[routing.DataParallelRankHeader],
		"the rank of the selected endpoint must be pinned")
}

func TestDPRankHeaderHandlerDoesNotPinWithoutSelectedEndpoint(t *testing.T) {
	tests := []struct {
		name   string
		result *scheduling.SchedulingResult
	}{
		{"nil scheduling result", nil},
		{"no endpoints selected", &scheduling.SchedulingResult{
			PrimaryProfileName: dpTestProfile,
			ProfileResults:     map[string]*scheduling.ProfileRunResult{dpTestProfile: {}},
		}},
		{"missing primary profile result", &scheduling.SchedulingResult{
			PrimaryProfileName: dpTestProfile,
			ProfileResults:     map[string]*scheduling.ProfileRunResult{},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := runDPRankHandler(t, map[string]string{},
				map[string]int{"10.0.0.1:8000": 2}, tt.result)

			assert.NotContains(t, headers, routing.DataParallelRankHeader)
		})
	}
}

// TestDPRankHeaderHandlerClearsClientSuppliedRank stops a caller pinning itself
// to a rank of its choosing and steering around the router's cache-aware
// placement.
func TestDPRankHeaderHandlerClearsClientSuppliedRank(t *testing.T) {
	headers := runDPRankHandler(t, map[string]string{
		routing.DataParallelRankHeader: "7",
	}, nil, dpResultForFirstEndpoint())

	assert.NotContains(t, headers, routing.DataParallelRankHeader,
		"a client-supplied rank must not survive")
}

// TestDPRankHeaderHandlerNoOpWithoutRecordedRanks covers requests for which the
// precise producer exposed no shared-port rank.
func TestDPRankHeaderHandlerNoOpWithoutRecordedRanks(t *testing.T) {
	headers := runDPRankHandler(t, map[string]string{}, nil, dpResultForFirstEndpoint())
	assert.Empty(t, headers)
}

// TestDPRankHeaderHandlerSelectedEndpointHasNoRank covers a pod that was scored
// but held no data-parallel entries, so there is no rank to pin.
func TestDPRankHeaderHandlerSelectedEndpointHasNoRank(t *testing.T) {
	headers := runDPRankHandler(t, map[string]string{}, map[string]int{
		"10.0.0.9:8000": 5,
	}, dpResultForFirstEndpoint())

	assert.NotContains(t, headers, routing.DataParallelRankHeader)
}

func TestDPRankHeaderHandlerToleratesNilRequest(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.NoError(t,
			NewDPRankHeaderHandler().PreRequest(context.Background(), nil, dpResultForFirstEndpoint()))
		assert.NoError(t, NewDPRankHeaderHandler().PreRequest(context.Background(),
			&scheduling.InferenceRequest{}, dpResultForFirstEndpoint()))
	})
}

func TestDPRankHeaderHandlerRecordsRoutingDecision(t *testing.T) {
	dpRankRoutingTotal.Reset()
	t.Cleanup(dpRankRoutingTotal.Reset)

	handler := NewDPRankHeaderHandler().WithName("test-handler")
	pinned := &scheduling.InferenceRequest{Headers: map[string]string{}}
	pinned.PutAttribute(preciseprefixcacheconstants.WinningRanksDataKey, map[string]int{
		"10.0.0.1:8000": 2,
	})
	require.NoError(t, handler.PreRequest(context.Background(), pinned, dpResultForFirstEndpoint()))

	internal := &scheduling.InferenceRequest{Headers: map[string]string{}}
	require.NoError(t, handler.PreRequest(context.Background(), internal, dpResultForFirstEndpoint()))

	assert.Equal(t, float64(1), testutil.ToFloat64(dpRankRoutingTotal.WithLabelValues(
		DPRankHeaderHandlerType, "test-handler", routingDecisionPreciseKV, "2")))
	assert.Equal(t, float64(1), testutil.ToFloat64(dpRankRoutingTotal.WithLabelValues(
		DPRankHeaderHandlerType, "test-handler", routingDecisionVLLMInternal, noDataParallelRankLabel)))
}
