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

package preciseprefixcache

import (
	"context"
	"testing"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestProbeReusesKeysAndRefreshesMatches(t *testing.T) {
	computes, matches := 0, 0
	index := &fakeKVCacheIndexer{
		computeFromTokens: func(context.Context, []uint32, string, []*kvblock.BlockExtraFeatures) ([]kvblock.BlockHash, error) {
			computes++
			return []kvblock.BlockHash{1}, nil
		},
		matchBlockKeys: func(context.Context, []kvblock.BlockHash, sets.Set[string]) (map[string]kvcache.PodMatch, error) {
			matches++
			return map[string]kvcache.PodMatch{}, nil
		},
	}
	producer := newProducerWithIndexer(t.Context(), index)
	probe := producer.NewProbeDataProducer()
	request := &scheduling.InferenceRequest{TargetModel: "a", Body: &fwkrh.InferenceRequestBody{TokenizedRequest: &scheduling.TokenizedRequest{Prompts: []fwkrh.PromptTokens{{TokenIDs: make([]uint32, 64)}}}}}
	endpoints := []scheduling.Endpoint{scheduling.NewEndpoint(&fwkdl.EndpointMetadata{Address: "10.0.0.1", Port: "8080"}, nil, nil)}
	require.NoError(t, probe.Produce(t.Context(), request, endpoints))
	copy := *request.Body.TokenizedRequest
	request.Body.TokenizedRequest = &copy
	require.NoError(t, probe.Produce(t.Context(), request, endpoints))
	require.Equal(t, 1, computes)
	require.Equal(t, 2, matches)
	request.Body.TokenizedRequest.CacheSalt = "salt"
	require.NoError(t, probe.Produce(t.Context(), request, endpoints))
	require.Equal(t, 2, computes)
	request.TargetModel = "b"
	require.NoError(t, probe.Produce(t.Context(), request, endpoints))
	require.Equal(t, 3, computes)
	request.Body.SkipPrefixCacheMatching = true
	require.NoError(t, probe.Produce(t.Context(), request, endpoints))
	require.Equal(t, 3, computes)
	require.Equal(t, 4, matches)
}
