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

package dataparallel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func TestDPRankHeaderHandlerPinsSelectedLogicalRank(t *testing.T) {
	rank := 4
	endpoint := scheduling.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:               k8stypes.NamespacedName{Namespace: "default", Name: "pod-1-rank-4"},
		Address:          "10.0.0.1",
		Port:             DefaultTestPodPort,
		DataParallelRank: &rank,
	}, nil, nil)
	request := &scheduling.InferenceRequest{Headers: map[string]string{
		routing.DataParallelRankHeader: "7",
	}}
	result := &scheduling.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*scheduling.ProfileRunResult{
			"default": {TargetEndpoints: []scheduling.Endpoint{endpoint}},
		},
	}

	require.NoError(t, NewDPRankHeaderHandler().PreRequest(context.Background(), request, result))
	assert.Equal(t, "4", request.Headers[routing.DataParallelRankHeader])
}

func TestDPRankHeaderHandlerRemovesUntrustedRank(t *testing.T) {
	request := &scheduling.InferenceRequest{Headers: map[string]string{
		routing.DataParallelRankHeader: "7",
	}}
	result := &scheduling.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*scheduling.ProfileRunResult{
			"default": {TargetEndpoints: []scheduling.Endpoint{
				createEndpoint(k8stypes.NamespacedName{Name: "pod-1"}, "10.0.0.1", DefaultTestPodPort, nil),
			}},
		},
	}

	require.NoError(t, NewDPRankHeaderHandler().PreRequest(context.Background(), request, result))
	assert.NotContains(t, request.Headers, routing.DataParallelRankHeader)
}

func TestDPRankHeaderHandlerToleratesMissingSelection(t *testing.T) {
	request := &scheduling.InferenceRequest{}
	require.NoError(t, NewDPRankHeaderHandler().PreRequest(context.Background(), request, nil))
	assert.Empty(t, request.Headers)
}
