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
	"strconv"
	"testing"
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/fairness/globalstrict"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/fairness/roundrobin"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

type lookaheadOrdering struct{}

func (lookaheadOrdering) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "lookahead-ordering"}
}
func (lookaheadOrdering) Less(a, b flowcontrol.QueueItemAccessor) bool {
	return a.EnqueueTime().Before(b.EnqueueTime())
}

func TestLookaheadPreservesSkippedRequests(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	q := h.addQueue(testFlow)
	q.OrderingPolicyFunc = func() flowcontrol.OrderingPolicy { return lookaheadOrdering{} }
	h.endpointCandidates.Candidates = []fwkdl.Endpoint{
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RolePrefill}}, nil),
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RoleDecode}}, nil),
	}
	prefill := 1.2
	h.saturationDetector.SaturationFunc = func(ctx context.Context, _ []fwkdl.Endpoint) float64 {
		if flowcontrol.SaturationStageFromContext(ctx) == flowcontrol.SaturationStagePrefill {
			return prefill
		}
		return 0.2
	}
	items := make([]*FlowItem, 3)
	commits := make([]int, 3)
	for i := range items {
		req := &preparedRequest{FlowControlRequest: fwkfcmocks.NewMockFlowControlRequest(100, string(rune('a'+i)), testFlow)}
		req.prepare = func(context.Context) (*flowcontrol.DispatchPlan, error) {
			stages := []string{flowcontrol.SaturationStageDecode}
			if i < 2 {
				stages = append(stages, flowcontrol.SaturationStagePrefill)
			}
			return &flowcontrol.DispatchPlan{Stages: stages, Commit: func() { commits[i]++ }}, nil
		}
		items[i] = NewItem(req, testTTL, h.clock.Now(), logr.Discard())
		require.NoError(t, q.Add(items[i]))
		h.clock.Step(time.Millisecond)
	}
	handles := []flowcontrol.QueueItemHandle{items[0].Handle(), items[1].Handle()}
	enqueued := []time.Time{items[0].EnqueueTime(), items[1].EnqueueTime()}
	require.True(t, h.processor.dispatchCycle(context.Background()))
	require.Equal(t, []int{0, 0, 1}, commits)
	require.Equal(t, 2, q.Len())
	require.Equal(t, uint64(200), q.ByteSize())
	for i := range handles {
		require.Same(t, handles[i], items[i].Handle())
		require.Equal(t, enqueued[i], items[i].EnqueueTime())
		require.Equal(t, testTTL, items[i].EffectiveTTL())
		require.Nil(t, items[i].FinalState())
	}
	prefill = 0.2
	require.True(t, h.processor.dispatchCycle(context.Background()))
	require.Equal(t, []int{0, 0, 1}, commits)
	require.NotNil(t, items[0].FinalState())
	require.Same(t, items[1], q.Peek())
}

func TestLookaheadAcrossFlows(t *testing.T) {
	for _, policyName := range []string{"global-strict", "round-robin"} {
		t.Run(policyName, func(t *testing.T) {
			h := newTestHarness(t, testCleanupTick)
			var fairness flowcontrol.FairnessPolicy
			if policyName == "global-strict" {
				p, err := globalstrict.GlobalStrictFairnessPolicyFactory("test", nil, nil)
				require.NoError(t, err)
				fairness = p.(flowcontrol.FairnessPolicy)
			} else {
				p, err := roundrobin.RoundRobinFairnessPolicyFactory("test", nil, nil)
				require.NoError(t, err)
				fairness = p.(flowcontrol.FairnessPolicy)
			}
			state := fairness.NewState(context.Background())
			h.FairnessPolicyFunc = func(int) (flowcontrol.FairnessPolicy, error) { return fairness, nil }
			h.PriorityBandAccessorFunc = func(priority int) (flowcontrol.PriorityBandAccessor, error) {
				band, err := h.priorityBandAccessor(priority)
				if err == nil {
					mockBand := band.(*fwkfcmocks.MockPriorityBandAccessor)
					mockBand.PolicyStateV = state
					mockBand.FlowKeysFunc = func() []flowcontrol.FlowKey {
						var keys []flowcontrol.FlowKey
						band.IterateQueues(func(q flowcontrol.FlowQueueAccessor) bool { keys = append(keys, q.FlowKey()); return true })
						return keys
					}
					mockBand.QueueFunc = func(id string) flowcontrol.FlowQueueAccessor {
						var found flowcontrol.FlowQueueAccessor
						band.IterateQueues(func(q flowcontrol.FlowQueueAccessor) bool {
							if q.FlowKey().ID == id {
								found = q
								return false
							}
							return true
						})
						return found
					}
				}
				return band, err
			}
			blockedFlow := flowcontrol.FlowKey{ID: "a", Priority: testFlow.Priority}
			readyFlow := flowcontrol.FlowKey{ID: "b", Priority: testFlow.Priority}
			blockedQ, readyQ := h.addQueue(blockedFlow), h.addQueue(readyFlow)
			blockedQ.OrderingPolicyFunc = func() flowcontrol.OrderingPolicy { return lookaheadOrdering{} }
			readyQ.OrderingPolicyFunc = func() flowcontrol.OrderingPolicy { return lookaheadOrdering{} }
			blockedReq := &preparedRequest{FlowControlRequest: fwkfcmocks.NewMockFlowControlRequest(100, "blocked", blockedFlow),
				prepare: pendingPreparation}
			blocked := NewItem(blockedReq, testTTL, h.clock.Now(), logr.Discard())
			require.NoError(t, blockedQ.Add(blocked))
			h.clock.Step(time.Millisecond)
			ready := h.newTestItem("ready", readyFlow, testTTL)
			require.NoError(t, readyQ.Add(ready))
			require.True(t, h.processor.dispatchCycle(context.Background()))
			require.NotNil(t, ready.FinalState())
			require.Nil(t, blocked.FinalState())
			require.Equal(t, 1, blockedQ.Len())
		})
	}
}

func TestLookaheadGlobalOrderingUsesEligibleCandidates(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	policy, err := globalstrict.GlobalStrictFairnessPolicyFactory("test", nil, nil)
	require.NoError(t, err)
	h.FairnessPolicyFunc = func(int) (flowcontrol.FairnessPolicy, error) { return policy.(flowcontrol.FairnessPolicy), nil }
	otherFlow := flowcontrol.FlowKey{ID: "other", Priority: testFlow.Priority}
	qa, qb := h.addQueue(testFlow), h.addQueue(otherFlow)
	qa.OrderingPolicyFunc = func() flowcontrol.OrderingPolicy { return lookaheadOrdering{} }
	qb.OrderingPolicyFunc = func() flowcontrol.OrderingPolicy { return lookaheadOrdering{} }
	blockedReq := &preparedRequest{FlowControlRequest: fwkfcmocks.NewMockFlowControlRequest(100, "blocked", testFlow), prepare: pendingPreparation}
	require.NoError(t, qa.Add(NewItem(blockedReq, testTTL, h.clock.Now(), logr.Discard())))
	h.clock.Step(time.Millisecond)
	earlier := h.newTestItem("earlier", otherFlow, testTTL)
	require.NoError(t, qb.Add(earlier))
	h.clock.Step(time.Millisecond)
	later := h.newTestItem("later", testFlow, testTTL)
	require.NoError(t, qa.Add(later))
	require.True(t, h.processor.dispatchCycle(context.Background()))
	require.NotNil(t, earlier.FinalState())
	require.Nil(t, later.FinalState())
	require.True(t, h.processor.dispatchCycle(context.Background()))
	require.NotNil(t, later.FinalState())
}

func TestLookaheadSkipsExpiredAndCancelledItems(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	h.endpointCandidates.Candidates = []fwkdl.Endpoint{
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RolePrefill}}, nil),
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RoleDecode}}, nil),
	}
	h.saturationDetector.SaturationFunc = func(ctx context.Context, _ []fwkdl.Endpoint) float64 {
		if flowcontrol.SaturationStageFromContext(ctx) == flowcontrol.SaturationStagePrefill {
			return 1.2
		}
		return 0.2
	}
	makeItem := func(id string, ttl time.Duration) *FlowItem {
		item := h.newTestItem(id, testFlow, ttl)
		item.originalRequest = &preparedRequest{FlowControlRequest: item.OriginalRequest(), prepare: func(context.Context) (*flowcontrol.DispatchPlan, error) {
			return &flowcontrol.DispatchPlan{Stages: []string{flowcontrol.SaturationStageDecode}}, nil
		}}
		return item
	}
	q := h.addQueue(testFlow)
	q.OrderingPolicyFunc = func() flowcontrol.OrderingPolicy { return lookaheadOrdering{} }
	expired := makeItem("expired", time.Millisecond)
	require.NoError(t, q.Add(expired))
	h.clock.Step(2 * time.Millisecond)
	cancelled := makeItem("cancelled", testTTL)
	require.NoError(t, q.Add(cancelled))
	cancelled.Finalize(context.Canceled)
	h.clock.Step(time.Millisecond)
	ready := makeItem("ready", testTTL)
	require.NoError(t, q.Add(ready))
	require.True(t, h.processor.dispatchCycle(context.Background()))
	require.NotNil(t, ready.FinalState())
	require.Error(t, expired.FinalState().Err)
	require.Error(t, cancelled.FinalState().Err)
	h.processor.sweepFinalizedItems()
	require.Zero(t, q.Len())
	require.Zero(t, q.ByteSize())
}

func pendingPreparation(context.Context) (*flowcontrol.DispatchPlan, error) {
	return nil, nil //nolint:nilnil // A nil plan represents pending preparation.
}

func TestLookaheadBudgetIsSharedAcrossBands(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	h.endpointCandidates.Candidates = []fwkdl.Endpoint{
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RolePrefill}}, nil),
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{bylabel.RoleLabel: bylabel.RoleDecode}}, nil),
	}
	h.saturationDetector.SaturationFunc = func(ctx context.Context, _ []fwkdl.Endpoint) float64 {
		if flowcontrol.SaturationStageFromContext(ctx) == flowcontrol.SaturationStagePrefill {
			return 1.2
		}
		return 0.2
	}
	probes := 0
	var last *FlowItem
	for band := range 2 {
		key := flowcontrol.FlowKey{ID: strconv.Itoa(band), Priority: 10 - band}
		q := h.addQueue(key)
		q.OrderingPolicyFunc = func() flowcontrol.OrderingPolicy { return lookaheadOrdering{} }
		for i := range 20 {
			req := &preparedRequest{FlowControlRequest: fwkfcmocks.NewMockFlowControlRequest(100, strconv.Itoa(band*20+i), key)}
			req.prepare = func(context.Context) (*flowcontrol.DispatchPlan, error) {
				probes++
				if band == 1 && i == 19 {
					return &flowcontrol.DispatchPlan{Stages: []string{flowcontrol.SaturationStageDecode}}, nil
				}
				return nil, nil //nolint:nilnil // A nil plan keeps the request queued.
			}
			last = NewItem(req, testTTL, h.clock.Now(), logr.Discard())
			require.NoError(t, q.Add(last))
			h.clock.Step(time.Millisecond)
		}
	}
	require.False(t, h.processor.dispatchCycle(context.Background()))
	require.Equal(t, maxLookaheadCandidates, probes)
	require.Nil(t, last.FinalState())
}

func TestHealthySelectionDoesNotConsumeLookaheadBudget(t *testing.T) {
	h := newTestHarness(t, testCleanupTick)
	item := h.newTestItem("ordinary", testFlow, testTTL)
	require.NoError(t, h.addQueue(testFlow).Add(item))
	band, err := h.priorityBandAccessor(testFlow.Priority)
	require.NoError(t, err)
	remaining := maxLookaheadCandidates
	signals := map[string]float64{flowcontrol.SaturationStageOverall: 0.2, flowcontrol.SaturationStageDecode: 0.2, flowcontrol.SaturationStagePrefill: 0.2}
	limits := map[string][]float64{flowcontrol.SaturationStageOverall: {1}, flowcontrol.SaturationStageDecode: {1}, flowcontrol.SaturationStagePrefill: {1}}
	got, plan, err := h.processor.selectPreparedItem(t.Context(), band, 0, &remaining, signals, limits)
	require.NoError(t, err)
	require.Same(t, item, got)
	require.Nil(t, plan)
	require.Equal(t, maxLookaheadCandidates, remaining)
}
