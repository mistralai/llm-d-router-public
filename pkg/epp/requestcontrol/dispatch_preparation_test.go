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
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/contracts/mocks"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/profilehandler/disagg"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
	schedulerpkg "github.com/llm-d/llm-d-router/pkg/epp/scheduling"
)

type dispatchTestProfile struct {
	name string
	runs *atomic.Int64
}

func (p dispatchTestProfile) Run(_ context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) (*fwksched.ProfileRunResult, error) {
	if p.runs != nil {
		p.runs.Add(1)
	}
	for _, endpoint := range endpoints {
		if endpoint.GetMetadata().ID.Name == p.name {
			return &fwksched.ProfileRunResult{TargetEndpoints: []fwksched.Endpoint{endpoint}}, nil
		}
	}
	return nil, errors.New("test endpoint is unavailable")
}

func (p dispatchTestProfile) FilterCandidates(_ context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	var filtered []fwksched.Endpoint
	for _, endpoint := range endpoints {
		if endpoint.GetMetadata().ID.Name == p.name {
			filtered = append(filtered, endpoint)
		}
	}
	return filtered
}

type dispatchPrefixProducer struct{ cached atomic.Int64 }

func (*dispatchPrefixProducer) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "dispatch-test-prefix", Name: "dispatch-test-prefix"}
}
func (*dispatchPrefixProducer) Produces() map[plugin.DataKey]any {
	return map[plugin.DataKey]any{attrprefix.PrefixCacheMatchInfoDataKey: attrprefix.PrefixCacheMatchInfo{}}
}
func (p *dispatchPrefixProducer) Produce(_ context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	for _, endpoint := range endpoints {
		endpoint.Put(attrprefix.PrefixCacheMatchInfoDataKey, attrprefix.NewPrefixCacheMatchInfo(int(p.cached.Load()), 10, 1))
	}
	return nil
}

func TestDispatchPreparationRefreshesPrefixAndRetainsPlacement(t *testing.T) {
	producer := &dispatchPrefixProducer{}
	decider, err := disagg.NewPrefixBasedPDDecider(disagg.PrefixBasedPDDeciderConfig{NonCachedTokens: 4})
	require.NoError(t, err)
	handler := disagg.NewDisaggProfileHandler("decode", "prefill", "", decider, nil)
	datalayer.RegisterScopeSpecs([]plugin.Plugin{producer, handler, decider})
	var decodeRuns, prefillRuns atomic.Int64
	scheduler := schedulerpkg.NewSchedulerWithConfig(schedulerpkg.NewSchedulerConfig(handler, map[string]fwksched.SchedulerProfile{
		"decode": dispatchTestProfile{"decode", &decodeRuns}, "prefill": dispatchTestProfile{"prefill", &prefillRuns},
	}))
	candidates := &mocks.MockEndpointCandidates{Candidates: []fwkdl.Endpoint{
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "decode"}}, nil),
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "prefill"}}, nil),
	}}
	d := &Director{scheduler: scheduler, endpointCandidates: candidates, requestControlPlugins: Config{dataProducerPlugins: []fwkrc.DataProducer{producer}}}
	reqCtx := newSeamRequestContext("prefix-refresh")
	reqCtx.SchedulingRequest.Body = &fwkrh.InferenceRequestBody{TokenizedRequest: &fwksched.TokenizedRequest{Prompts: []fwkrh.PromptTokens{{TokenIDs: make([]uint32, 10)}}}}
	var committed *preparedScheduling
	prepare := d.newDispatchPreparation(t.Context(), reqCtx, func(result *preparedScheduling) { committed = result })
	poll := func() *flowcontrol.DispatchPlan {
		t.Helper()
		var plan *flowcontrol.DispatchPlan
		require.Eventually(t, func() bool {
			var err error
			plan, err = prepare(t.Context())
			require.NoError(t, err)
			return plan != nil
		}, time.Second, time.Millisecond)
		return plan
	}
	miss := d.prepareScheduling(t.Context(), scheduler, copyDispatchRequest(reqCtx.SchedulingRequest), nil)
	require.NoError(t, miss.err)
	require.Nil(t, miss.prepared)
	require.Nil(t, committed)
	require.Empty(t, reqCtx.SchedulingRequest.AttributeKeys())
	producer.cached.Store(10)
	second := poll()
	require.Zero(t, decodeRuns.Load())
	require.Zero(t, prefillRuns.Load())
	require.Equal(t, []string{flowcontrol.SaturationStageDecode}, second.Stages)
	ready, err := second.Complete()
	require.NoError(t, err)
	require.True(t, ready)
	second.Commit()
	require.Equal(t, int64(1), decodeRuns.Load())
	require.Zero(t, prefillRuns.Load())
	require.NotNil(t, committed)
	require.Equal(t, "decode", committed.result.PrimaryEndpoint().GetMetadata().ID.Name)
	require.Nil(t, committed.result.ProfileResults["prefill"])
	require.NotSame(t, reqCtx.SchedulingRequest, committed.request)
	require.NotSame(t, reqCtx.SchedulingRequest.Body, committed.request.Body)
}

type handoffScheduler struct {
	mockScheduler
	calls  atomic.Int64
	probes atomic.Int64
}

func (*handoffScheduler) SupportsDispatchPreparation() bool { return true }
func (*handoffScheduler) DispatchUsesPrefill(*fwksched.InferenceRequest, *fwksched.SchedulingResult) bool {
	return false
}
func (s *handoffScheduler) Schedule(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) (*fwksched.SchedulingResult, error) {
	s.calls.Add(1)
	request.Body.TokenizedRequest = &fwksched.TokenizedRequest{Prompts: []fwkrh.PromptTokens{{TokenIDs: []uint32{1, 2}}}}
	request.Headers["prepared"] = "yes"
	return s.mockScheduler.Schedule(ctx, request, endpoints)
}

type handoffAdmission struct{ mockAdmissionController }

func (*handoffAdmission) AdmitPrepared(ctx context.Context, _ *handlers.RequestContext, _ int, prepare flowcontrol.PrepareDispatchFunc) error {
	for {
		plan, err := prepare(ctx)
		if err != nil {
			return err
		}
		if plan != nil {
			if plan.Complete != nil {
				ready, err := plan.Complete()
				if err != nil {
					return err
				}
				if !ready {
					continue
				}
			}
			plan.Commit()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func TestDirectorPreparedPlacementHandoff(t *testing.T) {
	endpoint := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
		ID: k8stypes.NamespacedName{Name: "pod1", Namespace: "default"}, Address: "192.168.1.100", Port: "8000",
	}, nil, nil)
	result := &fwksched.SchedulingResult{PrimaryProfileName: "decode", ProfileResults: map[string]*fwksched.ProfileRunResult{"decode": {TargetEndpoints: []fwksched.Endpoint{endpoint}}}}
	director, ctx := newSinglePodDirector(t, result)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result.ProfileResults["decode"].TargetEndpoints = director.toSchedulerEndpoints(director.endpointCandidates.Locate(ctx, nil))
	scheduler := &handoffScheduler{mockScheduler: mockScheduler{scheduleResults: result}}
	director.scheduler = scheduler
	director.admissionController = &handoffAdmission{}
	body, err := json.Marshal(map[string]any{"model": "m", "prompt": "p"})
	require.NoError(t, err)
	reqCtx := &handlers.RequestContext{Request: &handlers.Request{ID: "handoff", Headers: map[string]string{":path": "/v1/completions"}, RawBody: body}, Parser: openai.NewOpenAIParser()}
	parsed, err := reqCtx.Parser.ParseRequest(ctx, body, reqCtx.Request.Headers)
	require.NoError(t, err)
	preRequests := 0
	director.requestControlPlugins = *NewConfig().WithPreRequestPlugins(&mockPreRequestPlugin{name: "handoff-observer", modifyFn: func(request *fwksched.InferenceRequest) {
		preRequests++
		require.Same(t, parsed.Body, request.Body)
		require.Equal(t, "yes", request.Headers["prepared"])
		require.Same(t, result, request.SchedulingResult)
	}})
	_, err = director.HandleRequest(ctx, reqCtx, parsed.Body)
	require.NoError(t, err)
	require.Equal(t, int64(1), scheduler.calls.Load())
	require.Equal(t, 1, preRequests)
	require.NotNil(t, parsed.Body.TokenizedRequest)
	require.Equal(t, "yes", reqCtx.Request.Headers["prepared"])
	require.Equal(t, "192.168.1.100:8000", reqCtx.TargetEndpoint)
}

func TestDispatchPreparationReservesSharedCapacity(t *testing.T) {
	endpoint := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RolePrefillDecode}}, nil, nil)
	result := &fwksched.SchedulingResult{PrimaryProfileName: "decode", ProfileResults: map[string]*fwksched.ProfileRunResult{"decode": {TargetEndpoints: []fwksched.Endpoint{endpoint}}}}
	scheduler := &handoffScheduler{mockScheduler: mockScheduler{scheduleResults: result}}
	director := &Director{scheduler: scheduler, endpointCandidates: &mocks.MockEndpointCandidates{Candidates: []fwkdl.Endpoint{fwkdl.NewEndpoint(endpoint.GetMetadata(), nil)}}}
	request := newSeamRequestContext("shared").SchedulingRequest
	request.Headers = map[string]string{}
	request.Body = &fwkrh.InferenceRequestBody{}
	prepared := director.prepareScheduling(t.Context(), scheduler, request, nil)
	require.NoError(t, prepared.err)
	require.Equal(t, []string{flowcontrol.SaturationStageDecode}, prepared.stages)
	require.Equal(t, []string{flowcontrol.SaturationStageDecode, flowcontrol.SaturationStagePrefill}, prepared.reservationStages)
}

func TestDispatchPreparationSurvivesDelayedPoll(t *testing.T) {
	endpoint := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "decode"}}, nil)
	result := &fwksched.SchedulingResult{PrimaryProfileName: "decode", ProfileResults: map[string]*fwksched.ProfileRunResult{"decode": {TargetEndpoints: []fwksched.Endpoint{fwksched.NewEndpoint(endpoint.GetMetadata(), nil, nil)}}}}
	scheduler := &handoffScheduler{mockScheduler: mockScheduler{scheduleResults: result}}
	candidates := &mocks.MockEndpointCandidates{Candidates: []fwkdl.Endpoint{endpoint}}
	director := &Director{scheduler: scheduler, endpointCandidates: candidates}
	reqCtx := newSeamRequestContext("delayed")
	reqCtx.SchedulingRequest.Body = &fwkrh.InferenceRequestBody{}
	reqCtx.SchedulingRequest.Headers = map[string]string{}
	prepare := director.newDispatchPreparation(t.Context(), reqCtx, func(*preparedScheduling) {})
	plan, err := prepare(t.Context())
	require.NoError(t, err)
	require.Nil(t, plan)
	require.Eventually(t, func() bool { return scheduler.probes.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	plan, err = prepare(t.Context())
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Zero(t, scheduler.calls.Load())
	ready, err := plan.Complete()
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, int64(1), scheduler.calls.Load())
	candidates.Candidates = nil
	require.False(t, director.endpointsAvailable(t.Context(), nil, result.ProfileResults["decode"].TargetEndpoints))
}

func (s *handoffScheduler) ProbeDecodeLocal(_ context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) (fwksched.Endpoint, error) {
	s.probes.Add(1)
	if len(endpoints) == 0 {
		return nil, nil //nolint:nilnil
	}
	return endpoints[0], nil
}

func TestDispatchProbeDoesNotSchedule(t *testing.T) {
	endpoint := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "decode"}}, nil)
	result := &fwksched.SchedulingResult{PrimaryProfileName: "decode", ProfileResults: map[string]*fwksched.ProfileRunResult{"decode": {TargetEndpoints: []fwksched.Endpoint{fwksched.NewEndpoint(endpoint.GetMetadata(), nil, nil)}}}}
	scheduler := &handoffScheduler{mockScheduler: mockScheduler{scheduleResults: result}}
	director := &Director{scheduler: scheduler, endpointCandidates: &mocks.MockEndpointCandidates{Candidates: []fwkdl.Endpoint{endpoint}}}
	reqCtx := newSeamRequestContext("probe")
	reqCtx.SchedulingRequest.Body = &fwkrh.InferenceRequestBody{}
	reqCtx.SchedulingRequest.Headers = map[string]string{}
	prepare := director.newDispatchPreparation(t.Context(), reqCtx, func(*preparedScheduling) {})
	var plan *flowcontrol.DispatchPlan
	require.Eventually(t, func() bool {
		var err error
		plan, err = prepare(t.Context())
		require.NoError(t, err)
		return plan != nil
	}, time.Second, time.Millisecond)
	require.Zero(t, scheduler.calls.Load())
}

func TestPendingProbeResultsRetainCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	endpoint := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "decode"}}, nil)
	scheduler := &handoffScheduler{}
	director := &Director{scheduler: scheduler, endpointCandidates: &mocks.MockEndpointCandidates{Candidates: []fwkdl.Endpoint{endpoint}}}
	prepares := make([]flowcontrol.PrepareDispatchFunc, maxPendingDispatchPreparations+1)
	for i := range prepares {
		prepares[i] = director.newDispatchPreparation(ctx, newSeamRequestContext("pending"), func(*preparedScheduling) {})
		plan, err := prepares[i](ctx)
		require.NoError(t, err)
		require.Nil(t, plan)
	}
	require.Eventually(t, func() bool { return scheduler.probes.Load() == int64(maxPendingDispatchPreparations) }, time.Second, time.Millisecond)
	require.Equal(t, maxPendingDispatchPreparations, len(director.dispatchPreparationSlots))
	require.Zero(t, scheduler.calls.Load())
	var plan *flowcontrol.DispatchPlan
	require.Eventually(t, func() bool { var err error; plan, err = prepares[0](ctx); require.NoError(t, err); return plan != nil }, time.Second, time.Millisecond)
	require.Equal(t, maxPendingDispatchPreparations-1, len(director.dispatchPreparationSlots))
	_, err := prepares[len(prepares)-1](ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return scheduler.probes.Load() == int64(len(prepares)) }, time.Second, time.Millisecond)
	cancel()
	require.Eventually(t, func() bool { return len(director.dispatchPreparationSlots) == 0 }, time.Second, time.Millisecond)
}
