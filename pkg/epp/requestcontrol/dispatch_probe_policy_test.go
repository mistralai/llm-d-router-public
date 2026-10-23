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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/contracts/mocks"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"
	testclock "k8s.io/utils/clock/testing"
)

type probeMissScheduler struct {
	handoffScheduler
	misses atomic.Int64
}

func (s *probeMissScheduler) ProbeDecodeLocal(context.Context, *fwksched.InferenceRequest, []fwksched.Endpoint) (fwksched.Endpoint, error) {
	s.misses.Add(1)
	return nil, nil //nolint:nilnil
}

func TestDispatchProbeMissCooldown(t *testing.T) {
	clock := testclock.NewFakeClock(time.Now())
	scheduler := &probeMissScheduler{}
	director := &Director{scheduler: scheduler, dispatchProbeClock: clock, endpointCandidates: &mocks.MockEndpointCandidates{Candidates: []fwkdl.Endpoint{fwkdl.NewEndpoint(nil, nil)}}}
	request := newSeamRequestContext("cooldown")
	prepare := director.newDispatchPreparation(t.Context(), request, func(*preparedScheduling) {})
	_, err := prepare(t.Context())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := prepare(t.Context())
		require.NoError(t, err)
		return scheduler.misses.Load() == 1 && len(director.dispatchPreparationSlots) == 0
	}, time.Second, time.Millisecond)
	for range 20 {
		_, err := prepare(t.Context())
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), scheduler.misses.Load())
	clock.Step(dispatchProbeMissDelay - time.Nanosecond)
	_, err = prepare(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), scheduler.misses.Load())
	clock.Step(time.Nanosecond)
	_, err = prepare(t.Context())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return scheduler.misses.Load() == 2 }, time.Second, time.Millisecond)
}

func TestDecoderShortlistIsBoundedAndKeepsHint(t *testing.T) {
	candidates := make([]fwkdl.Endpoint, 0, 102)
	candidates = append(candidates, nil)
	for i := range 100 {
		candidates = append(candidates, fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: strconv.Itoa(i)}, Labels: map[string]string{bylabel.RoleLabel: bylabel.RoleDecode}}, nil))
	}
	prefill := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "prefill"}, Labels: map[string]string{bylabel.RoleLabel: bylabel.RolePrefill}}, nil)
	candidates = append(candidates, prefill)
	preferred := candidates[80].GetMetadata().ID
	for range 30 {
		selected := shortlistDecoders(candidates, preferred)
		require.Len(t, selected, dispatchProbeCandidates)
		require.Equal(t, preferred, selected[0].GetMetadata().ID)
		seen := map[fwkdl.ID]bool{}
		for _, endpoint := range selected {
			require.NotSame(t, prefill, endpoint)
			require.False(t, seen[endpoint.GetMetadata().ID])
			seen[endpoint.GetMetadata().ID] = true
		}
	}
}

type countedSourceAttributes struct {
	fwkdl.AttributeMap
	clones *atomic.Int64
}

func (a countedSourceAttributes) Clone() fwkdl.AttributeMap {
	a.clones.Add(1)
	return a.AttributeMap.Clone()
}

type countedSourceEndpoint struct {
	fwkdl.Endpoint
	attributes fwkdl.AttributeMap
}

func (e countedSourceEndpoint) GetAttributes() fwkdl.AttributeMap { return e.attributes }

func TestProbeShortlistsBeforeCloningWithoutScreeners(t *testing.T) {
	var clones atomic.Int64
	endpoints := make([]fwkdl.Endpoint, 0, 100)
	for i := range 100 {
		endpoint := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: strconv.Itoa(i)}}, nil)
		endpoints = append(endpoints, countedSourceEndpoint{Endpoint: endpoint, attributes: countedSourceAttributes{AttributeMap: fwkdl.NewAttributes(), clones: &clones}})
	}
	scheduler := &handoffScheduler{}
	director := &Director{scheduler: scheduler, endpointCandidates: &mocks.MockEndpointCandidates{Candidates: endpoints}}
	result := director.prepareScheduling(t.Context(), scheduler, newSeamRequestContext("shortlist").SchedulingRequest, nil)
	require.NoError(t, result.err)
	require.NotNil(t, result.prepared)
	require.Equal(t, int64(dispatchProbeCandidates), clones.Load())
}

var _ fwkdl.AttributeMap = countedSourceAttributes{}

func TestProbeUsesRequestMetadata(t *testing.T) {
	metadata := map[string]any{"subset": "tenant-pool"}
	director := &Director{endpointCandidates: &mocks.MockEndpointCandidates{LocateFunc: func(_ context.Context, got map[string]any) []fwkdl.Endpoint {
		require.Equal(t, metadata, got)
		return []fwkdl.Endpoint{fwkdl.NewEndpoint(nil, nil)}
	}}}
	result := director.prepareScheduling(t.Context(), &handoffScheduler{}, newSeamRequestContext("metadata").SchedulingRequest, metadata)
	require.NoError(t, result.err)
	require.NotNil(t, result.prepared)
}
