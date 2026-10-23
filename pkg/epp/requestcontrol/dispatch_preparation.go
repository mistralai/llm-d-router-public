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

package requestcontrol

import (
	"context"
	"errors"
	"maps"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
)

// Concurrent probes must not create one producer worker for every queued request.
const maxConcurrentDispatchPreparations = 32

type preparedAdmissionController interface {
	AdmitPrepared(context.Context, *handlers.RequestContext, int, flowcontrol.PrepareDispatchFunc) error
}

type dispatchScheduler interface {
	Scheduler
	fwksched.DispatchProfileHandler
}

type preparedScheduling struct {
	request   *fwksched.InferenceRequest
	endpoints []fwksched.Endpoint
	result    *fwksched.SchedulingResult
}

type preparationResult struct {
	prepared          *preparedScheduling
	stages            []string
	reservationStages []string
	err               error
	tokenized         *fwksched.TokenizedRequest
}

func (d *Director) supportsDispatchPreparation() bool {
	scheduler, ok := d.scheduler.(dispatchScheduler)
	if !ok {
		return false
	}
	return scheduler.SupportsDispatchPreparation()
}

func (d *Director) newDispatchPreparation(ctx context.Context, reqCtx *handlers.RequestContext, commit func(*preparedScheduling)) flowcontrol.PrepareDispatchFunc {
	d.dispatchPreparationOnce.Do(func() { d.dispatchPreparationSlots = make(chan struct{}, maxConcurrentDispatchPreparations) })
	base := reqCtx.SchedulingRequest
	metadata := reqCtx.Request.Metadata
	scheduler := d.scheduler.(dispatchScheduler)
	// Only the dispatch goroutine accesses pending. Each worker owns its request and endpoint snapshots.
	var pending chan preparationResult
	var tokenized *fwksched.TokenizedRequest
	return func(processorCtx context.Context) (*flowcontrol.DispatchPlan, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if pending != nil {
			select {
			case result := <-pending:
				pending = nil
				if result.err != nil {
					return nil, result.err
				}
				if result.tokenized != nil {
					tokenized = result.tokenized
				}
				if result.prepared == nil && len(result.stages) == 0 {
					return nil, nil
				}
				if result.prepared != nil && !d.placementAvailable(ctx, metadata, result.prepared.result) {
					return nil, nil
				}
				return &flowcontrol.DispatchPlan{Stages: result.stages, ReservationStages: result.reservationStages, Commit: func() {
					if result.prepared != nil {
						commit(result.prepared)
					}
				}}, nil
			default:
				return nil, nil
			}
		}
		select {
		case d.dispatchPreparationSlots <- struct{}{}:
		default:
			return nil, nil
		}
		request := copyDispatchRequest(base)
		if request.Body != nil && tokenized != nil {
			request.Body.TokenizedRequest = tokenized
		}
		pending = make(chan preparationResult, 1)
		resultCh := pending
		attemptCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(processorCtx, cancel)
		go func() {
			defer func() { <-d.dispatchPreparationSlots }()
			defer cancel()
			defer stop()
			result := d.prepareScheduling(attemptCtx, scheduler, request, metadata)
			resultCh <- result
		}()
		return nil, nil
	}
}

func copyDispatchRequest(base *fwksched.InferenceRequest) *fwksched.InferenceRequest {
	request := &fwksched.InferenceRequest{
		RequestID: base.RequestID, TargetModel: base.TargetModel,
		Headers: maps.Clone(base.Headers), Objectives: base.Objectives,
		FairnessID: base.FairnessID, RequestSizeBytes: base.RequestSizeBytes,
	}
	if base.Body != nil {
		body := *base.Body
		request.Body = &body
	}
	// Header-derived attributes survive retries. Placement attributes belong to the attempt that produces them.
	for _, key := range base.AttributeKeys() {
		value, _ := base.GetAttribute(key)
		request.PutAttribute(key, value)
	}
	return request
}

func (d *Director) prepareScheduling(ctx context.Context, scheduler dispatchScheduler, request *fwksched.InferenceRequest, metadata map[string]any) preparationResult {
	candidates := d.endpointCandidates.Locate(ctx, metadata)
	endpoints := d.runScreeners(ctx, request, d.toSchedulerEndpoints(candidates))
	if len(endpoints) == 0 {
		return preparationResult{}
	}
	if err := d.runDataProducerPlugins(ctx, request, endpoints); err != nil {
		if ctx.Err() != nil {
			return preparationResult{err: ctx.Err()}
		}
		// Failed preparation uses conservative admission and the director's ordinary producer error handling.
		return preparationResult{stages: []string{flowcontrol.SaturationStageDecode, flowcontrol.SaturationStagePrefill}}
	}
	prepared := preparationResult{}
	if request.Body != nil {
		prepared.tokenized = request.Body.TokenizedRequest
	}
	result, err := scheduler.Schedule(ctx, request, endpoints)
	if err != nil {
		var typed errcommon.Error
		if errors.As(err, &typed) && (typed.Code == errcommon.ServiceUnavailable || typed.Code == errcommon.ResourceExhausted) {
			return prepared
		}
		if errors.As(err, &typed) {
			prepared.err = err
		} else {
			prepared.err = errcommon.Error{Code: errcommon.ResourceExhausted, Msg: err.Error()}
		}
		return prepared
	}
	stages := []string{flowcontrol.SaturationStageDecode}
	if scheduler.DispatchUsesPrefill(request, result) {
		stages = append(stages, flowcontrol.SaturationStagePrefill)
	}
	prepared.prepared = &preparedScheduling{request: request, endpoints: endpoints, result: result}
	prepared.stages = stages
	if endpoint := result.PrimaryEndpoint(); endpoint != nil && endpoint.GetMetadata() != nil {
		switch endpoint.GetMetadata().Labels[bylabel.RoleLabel] {
		case bylabel.RolePrefillDecode, bylabel.RoleEncodePrefillDecode:
			prepared.reservationStages = []string{flowcontrol.SaturationStageDecode, flowcontrol.SaturationStagePrefill}
		}
	}
	return prepared
}

// Admission can wait across several policy cycles. Confirm that selected destinations still exist before publication.
func (d *Director) placementAvailable(ctx context.Context, metadata map[string]any, result *fwksched.SchedulingResult) bool {
	candidates := d.endpointCandidates.Locate(ctx, metadata)
	for _, profile := range result.ProfileResults {
		if profile == nil {
			continue
		}
		for _, selected := range profile.TargetEndpoints {
			if selected == nil || selected.GetMetadata() == nil {
				return false
			}
			wanted := selected.GetMetadata()
			found := false
			for _, candidate := range candidates {
				if candidate == nil || candidate.GetMetadata() == nil {
					continue
				}
				current := candidate.GetMetadata()
				if current.ID == wanted.ID && current.Address == wanted.Address && current.Port == wanted.Port {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}
