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

package requestcontrol

import (
	"context"
	"testing"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/contracts/mocks"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/saturationdetector/concurrency"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/maxscore"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/profilehandler/disagg"
	schedulerpkg "github.com/llm-d/llm-d-router/pkg/epp/scheduling"
	"github.com/llm-d/llm-d-router/test/utils"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

func TestDispatchCompletionChecksCurrentDecoderLoad(t *testing.T) {
	handle := utils.NewTestHandle(t.Context())
	detector, err := concurrency.ConcurrencyDetectorFactory("review-limit", plugin.StrictDecoder([]byte(`{"maxConcurrency":1,"failOpen":false}`)), handle)
	require.NoError(t, err)
	filter := detector.(fwksched.Filter)
	decider, err := disagg.NewPrefixBasedPDDecider(disagg.PrefixBasedPDDeciderConfig{NonCachedTokens: 4})
	require.NoError(t, err)
	handler := disagg.NewDisaggProfileHandler("decode", "prefill", "", decider, nil)
	producer := &dispatchPrefixProducer{}
	producer.cached.Store(10)
	picker := maxscore.NewMaxScorePicker(1)
	datalayer.RegisterScopeSpecs([]plugin.Plugin{detector, decider, handler, producer, picker})
	profile := schedulerpkg.NewSchedulerProfile().WithFilters(filter).WithPicker(picker)
	scheduler := schedulerpkg.NewSchedulerWithConfig(schedulerpkg.NewSchedulerConfig(handler, map[string]fwksched.SchedulerProfile{"decode": profile, "prefill": dispatchTestProfile{name: "prefill"}}))
	live := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "decoder"}}, nil)
	live.GetAttributes().Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{})
	director := &Director{scheduler: scheduler, endpointCandidates: &mocks.MockEndpointCandidates{Candidates: []fwkdl.Endpoint{live}}, requestControlPlugins: Config{dataProducerPlugins: []fwkrc.DataProducer{producer}}}
	request := newSeamRequestContext("stale-load").SchedulingRequest
	request.Body = &fwkrh.InferenceRequestBody{TokenizedRequest: &fwksched.TokenizedRequest{Prompts: []fwkrh.PromptTokens{{TokenIDs: make([]uint32, 10)}}}}
	prepared := director.prepareScheduling(t.Context(), scheduler, request, nil)
	require.NoError(t, prepared.err)
	require.NotNil(t, prepared.prepared)
	control := director.prepareScheduling(t.Context(), scheduler, copyDispatchRequest(request), nil)
	controlReady, controlErr := director.completeDispatch(t.Context(), scheduler, nil, control.prepared)
	require.NoError(t, controlErr)
	require.True(t, controlReady, "an unchanged idle decoder must be accepted")
	live.GetAttributes().Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{Requests: 1})
	fresh := director.toSchedulerEndpoints([]fwkdl.Endpoint{live})
	require.Empty(t, filter.Filter(t.Context(), request, fresh))
	ready, err := director.completeDispatch(t.Context(), scheduler, nil, prepared.prepared)
	require.NoError(t, err)
	require.False(t, ready, "the chosen decoder is at its configured concurrency limit")
}

type mutableDispatchScreener struct{ allow bool }

func (*mutableDispatchScreener) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "review-screener"}
}
func (s *mutableDispatchScreener) Screen(_ context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	if !s.allow {
		return nil
	}
	return endpoints
}

func TestDispatchCompletionRechecksMandatoryScreeners(t *testing.T) {
	live := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "decoder"}}, nil)
	endpoint := fwksched.NewEndpoint(live.GetMetadata(), nil, nil)
	result := &fwksched.SchedulingResult{PrimaryProfileName: "decode", ProfileResults: map[string]*fwksched.ProfileRunResult{"decode": {TargetEndpoints: []fwksched.Endpoint{endpoint}}}}
	scheduler := &handoffScheduler{mockScheduler: mockScheduler{scheduleResults: result}}
	screener := &mutableDispatchScreener{allow: true}
	director := &Director{scheduler: scheduler, endpointCandidates: &mocks.MockEndpointCandidates{Candidates: []fwkdl.Endpoint{live}}, requestControlPlugins: Config{screeners: []fwkrc.Screener{screener}}}
	request := newSeamRequestContext("screening").SchedulingRequest
	request.Headers = map[string]string{}
	request.Body = &fwkrh.InferenceRequestBody{}
	prepared := director.prepareScheduling(t.Context(), scheduler, request, nil)
	require.NoError(t, prepared.err)
	require.NotNil(t, prepared.prepared)
	screener.allow = false
	require.Empty(t, director.runScreeners(t.Context(), request, director.toSchedulerEndpoints([]fwkdl.Endpoint{live})))
	ready, err := director.completeDispatch(t.Context(), scheduler, nil, prepared.prepared)
	require.NoError(t, err)
	require.False(t, ready, "a destination rejected by the mandatory screener must not dispatch")
}
