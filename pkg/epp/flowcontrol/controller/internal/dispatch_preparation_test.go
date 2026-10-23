/*
Copyright 2025 The Kubernetes Authors.
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

package internal

import (
	"context"
	"testing"
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/types"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

type preparedRequest struct {
	flowcontrol.FlowControlRequest
	prepare flowcontrol.PrepareDispatchFunc
}

func (r *preparedRequest) DispatchPreparation() flowcontrol.PrepareDispatchFunc { return r.prepare }

func TestDispatchPreparationStageGating(t *testing.T) {
	for _, tc := range []struct {
		name             string
		prefill, decode  float64
		remote, dispatch bool
	}{
		{"local request bypasses saturated prefill", 1.2, 0.2, false, true},
		{"remote request waits for prefill", 1.2, 0.2, true, false},
		{"local request waits for decode", 0.2, 1.2, false, false},
		{"remote request uses both stages", 0.2, 0.2, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t, testCleanupTick)
			h.endpointCandidates.Candidates = []fwkdl.Endpoint{
				fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RolePrefill}}, nil),
				fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RoleDecode}}, nil),
			}
			h.saturationDetector.SaturationFunc = func(ctx context.Context, _ []fwkdl.Endpoint) float64 {
				if flowcontrol.SaturationStageFromContext(ctx) == flowcontrol.SaturationStagePrefill {
					return tc.prefill
				}
				return tc.decode
			}
			committed := false
			req := &preparedRequest{FlowControlRequest: fwkfcmocks.NewMockFlowControlRequest(100, tc.name, testFlow)}
			req.prepare = func(context.Context) (*flowcontrol.DispatchPlan, error) {
				stages := []string{flowcontrol.SaturationStageDecode}
				if tc.remote {
					stages = append(stages, flowcontrol.SaturationStagePrefill)
				}
				return &flowcontrol.DispatchPlan{Stages: stages, Commit: func() { committed = true }}, nil
			}
			item := NewItem(req, testTTL, h.clock.Now(), logr.Discard())
			q := h.addQueue(testFlow)
			require.NoError(t, q.Add(item))
			handle := item.Handle()
			enqueued := item.EnqueueTime()
			require.Equal(t, tc.dispatch, h.processor.dispatchCycle(context.Background()))
			require.Equal(t, tc.dispatch && tc.prefill >= 1, committed)
			if !tc.dispatch {
				require.Nil(t, item.FinalState())
				require.Same(t, handle, item.Handle())
				require.Equal(t, enqueued, item.EnqueueTime())
				require.Equal(t, testTTL, item.EffectiveTTL())
				require.Equal(t, 1, q.Len())
			}
		})
	}
}

func TestDispatchPreparationExpiryAndCancellation(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled", true: "expired"}[expired], func(t *testing.T) {
			h := newTestHarness(t, testCleanupTick)
			item := h.newTestItem("stopped", testFlow, testTTL)
			q := h.addQueue(testFlow)
			require.NoError(t, q.Add(item))
			if expired {
				h.clock.Step(testTTL + time.Millisecond)
			} else {
				item.Finalize(context.Canceled)
			}
			commits, reservations := 0, 0
			h.saturationDetector.ReserveDispatchFunc = func(string) bool { reservations++; return true }
			dispatched, err := h.processor.dispatchPreparedItem(item, &flowcontrol.DispatchPlan{
				Stages: []string{flowcontrol.SaturationStageDecode}, Commit: func() { commits++ },
			})
			require.NoError(t, err)
			require.False(t, dispatched)
			require.Zero(t, commits)
			require.Zero(t, reservations)
			require.NotNil(t, item.FinalState())
			if expired {
				require.ErrorIs(t, item.FinalState().Err, types.ErrTTLExpired)
			}
		})
	}
}

func TestDispatchPreparationCancellationAtReservation(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	item := h.newTestItem("cancel-at-reserve", testFlow, testTTL)
	require.NoError(t, h.addQueue(testFlow).Add(item))
	released, committed := false, false
	h.saturationDetector.ReserveDispatchFunc = func(string) bool { item.Finalize(context.Canceled); return true }
	h.saturationDetector.ReleaseDispatchFunc = func(string) bool { released = true; return true }
	dispatched, err := h.processor.dispatchPreparedItem(item, &flowcontrol.DispatchPlan{Commit: func() { committed = true }})
	require.NoError(t, err)
	require.False(t, dispatched)
	require.False(t, committed)
	require.True(t, released)
}

type sharedReservationDetector struct {
	*mockSaturationDetector
	stages []string
}

func (d *sharedReservationDetector) ReserveDispatchStages(_ string, stages []string) bool {
	d.stages = stages
	return true
}

func TestDispatchPreparationSharedReservation(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	detector := &sharedReservationDetector{mockSaturationDetector: h.saturationDetector}
	h.processor.saturationDetector = detector
	item := h.newTestItem("shared", testFlow, testTTL)
	require.NoError(t, h.addQueue(testFlow).Add(item))
	dispatched, err := h.processor.dispatchPreparedItem(item, &flowcontrol.DispatchPlan{
		Stages:            []string{flowcontrol.SaturationStageDecode},
		ReservationStages: []string{flowcontrol.SaturationStageDecode, flowcontrol.SaturationStagePrefill},
	})
	require.NoError(t, err)
	require.True(t, dispatched)
	require.Equal(t, []string{flowcontrol.SaturationStageDecode, flowcontrol.SaturationStagePrefill}, detector.stages)
}

func TestHealthyPDAdmissionDoesNotPrepare(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	h.endpointCandidates.Candidates = []fwkdl.Endpoint{
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RolePrefill}}, nil),
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RoleDecode}}, nil),
	}
	preparations := 0
	req := &preparedRequest{FlowControlRequest: fwkfcmocks.NewMockFlowControlRequest(100, "healthy", testFlow)}
	req.prepare = func(context.Context) (*flowcontrol.DispatchPlan, error) {
		preparations++
		return &flowcontrol.DispatchPlan{Stages: []string{flowcontrol.SaturationStageDecode}}, nil
	}
	item := NewItem(req, testTTL, h.clock.Now(), logr.Discard())
	require.NoError(t, h.addQueue(testFlow).Add(item))
	require.True(t, h.processor.dispatchCycle(context.Background()))
	require.Zero(t, preparations)
}

func TestDispatchCompletionFailureLeavesItemQueued(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	item := h.newTestItem("retry", testFlow, testTTL)
	q := h.addQueue(testFlow)
	require.NoError(t, q.Add(item))
	handle := item.Handle()
	committed := false
	dispatched, err := h.processor.dispatchPreparedItem(item, &flowcontrol.DispatchPlan{
		Complete: func() (bool, error) { return false, nil }, Commit: func() { committed = true },
	})
	require.NoError(t, err)
	require.False(t, dispatched)
	require.False(t, committed)
	require.Nil(t, item.FinalState())
	require.Same(t, handle, item.Handle())
	require.Equal(t, 1, q.Len())
}

func TestDispatchCompletionPreservesCancellationCause(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	item := h.newTestItem("cancel-completion", testFlow, testTTL)
	require.NoError(t, h.addQueue(testFlow).Add(item))
	dispatched, err := h.processor.dispatchPreparedItem(item, &flowcontrol.DispatchPlan{Complete: func() (bool, error) { return false, context.Canceled }})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, dispatched)
	require.ErrorIs(t, item.FinalState().Err, types.ErrContextCancelled)
}
