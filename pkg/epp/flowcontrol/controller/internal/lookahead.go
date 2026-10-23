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

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
)

type dispatchCandidate struct {
	flowcontrol.FlowQueueAccessor
	item flowcontrol.QueueItemAccessor
	plan *flowcontrol.DispatchPlan
}

func (q *dispatchCandidate) Peek() flowcontrol.QueueItemAccessor { return q.item }

// eligibleBand changes the policy view only. Queue occupancy and policy state stay with the registry.
type eligibleBand struct {
	flowcontrol.PriorityBandAccessor
	queues []*dispatchCandidate
	byID   map[string]*dispatchCandidate
}

func (b *eligibleBand) FlowKeys() []flowcontrol.FlowKey {
	keys := make([]flowcontrol.FlowKey, len(b.queues))
	for i, q := range b.queues {
		keys[i] = q.FlowKey()
	}
	return keys
}

func (b *eligibleBand) Queue(id string) flowcontrol.FlowQueueAccessor {
	q, found := b.byID[id]
	if !found {
		return nil
	}
	return q
}

func (b *eligibleBand) IterateQueues(visit func(flowcontrol.FlowQueueAccessor) bool) {
	for _, q := range b.queues {
		if !visit(q) {
			return
		}
	}
}

func (p *Processor) selectPreparedItem(ctx context.Context, band flowcontrol.PriorityBandAccessor,
	priorityIndex int, saturation map[string]float64, ceilings map[string][]float64,
) (flowcontrol.QueueItemAccessor, *flowcontrol.DispatchPlan, error) {
	view := &eligibleBand{PriorityBandAccessor: band, byID: make(map[string]*dispatchCandidate)}
	band.IterateQueues(func(queue flowcontrol.FlowQueueAccessor) bool {
		if ctx.Err() != nil {
			return false
		}
		if queue == nil {
			return true
		}
		first := queue.Peek()
		if first == nil {
			return true
		}
		plan, eligible := p.prepareItem(ctx, first, priorityIndex, saturation, ceilings)
		candidate := &dispatchCandidate{FlowQueueAccessor: queue, item: first, plan: plan}
		if !eligible {
			snapshotter, ok := queue.(flowcontrol.QueueSnapshotter)
			if !ok {
				return true
			}
			candidate = nil
			for _, item := range snapshotter.OrderedSnapshot() {
				if ctx.Err() != nil {
					return false
				}
				// The first item was already probed. Probing it twice could consume two different placement attempts.
				if item == first {
					continue
				}
				plan, eligible := p.prepareItem(ctx, item, priorityIndex, saturation, ceilings)
				if eligible {
					candidate = &dispatchCandidate{FlowQueueAccessor: queue, item: item, plan: plan}
					break
				}
			}
		}
		if candidate != nil {
			view.queues = append(view.queues, candidate)
			view.byID[queue.FlowKey().ID] = candidate
		}
		return true
	})
	item, err := p.selectItem(ctx, view)
	if err != nil || item == nil {
		return nil, nil, err
	}
	candidate := view.byID[item.OriginalRequest().FlowKey().ID]
	if candidate == nil || candidate.item != item {
		return nil, nil, nil
	}
	return item, candidate.plan, nil
}
