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
	"math/rand"
	"sync"
	"time"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
)

// Slots cover running probes and completed results that the dispatcher has not consumed.
const maxPendingDispatchPreparations = 32

// A bounded sample trades cache-hit coverage for less per-probe work.
const dispatchProbeCandidates = 4

// Failed probes wait before reading cache state again. Queue expiry uses a separate budget.
const dispatchProbeMissDelay = 10 * time.Millisecond

type preparedAdmissionController interface {
	AdmitPrepared(context.Context, *handlers.RequestContext, int, flowcontrol.PrepareDispatchFunc) error
}

type dispatchScheduler interface {
	Scheduler
	SupportsDispatchPreparation() bool
	DispatchUsesPrefill(*fwksched.InferenceRequest, *fwksched.SchedulingResult) bool
	ProbeDecodeLocal(context.Context, *fwksched.InferenceRequest, []fwksched.Endpoint) (fwksched.Endpoint, error)
}

type preparedScheduling struct {
	request    *fwksched.InferenceRequest
	endpoints  []fwksched.Endpoint
	result     *fwksched.SchedulingResult
	publishers []fwkrc.ProbeDataPublisher
}

type preparationResult struct {
	prepared          *preparedScheduling
	stages            []string
	reservationStages []string
	err               error
	tokenized         *fwksched.TokenizedRequest
}

type pendingPreparation struct {
	result  chan preparationResult
	release func()
}

func (d *Director) supportsDispatchPreparation() bool {
	scheduler, ok := d.scheduler.(dispatchScheduler)
	return ok && scheduler.SupportsDispatchPreparation()
}

func (d *Director) newDispatchPreparation(ctx context.Context, reqCtx *handlers.RequestContext, commit func(*preparedScheduling)) flowcontrol.PrepareDispatchFunc {
	d.dispatchPreparationOnce.Do(func() { d.dispatchPreparationSlots = make(chan struct{}, maxPendingDispatchPreparations) })
	base := reqCtx.SchedulingRequest
	metadata := reqCtx.Request.Metadata
	scheduler := d.scheduler.(dispatchScheduler)
	// Only the dispatch goroutine accesses pending. Each worker owns its request and endpoint snapshots.
	var pending *pendingPreparation
	var tokenized *fwksched.TokenizedRequest
	var retryAfter time.Time
	var preferred fwkdl.ID
	var producers []fwkrc.DataProducer
	producersReady := false
	now := time.Now
	if d.dispatchProbeClock != nil {
		now = d.dispatchProbeClock.Now
	}
	return func(processorCtx context.Context) (*flowcontrol.DispatchPlan, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if pending != nil {
			select {
			case result := <-pending.result:
				pending.release()
				pending = nil
				if result.err != nil {
					return nil, result.err
				}
				if result.tokenized != nil {
					tokenized = result.tokenized
				}
				if result.prepared == nil || !d.endpointsAvailable(ctx, metadata, result.prepared.endpoints) {
					retryAfter = now().Add(dispatchProbeMissDelay)
					return nil, nil
				}
				preferred = result.prepared.endpoints[0].GetMetadata().ID
				return &flowcontrol.DispatchPlan{
					Stages: result.stages, ReservationStages: result.reservationStages,
					Complete: func() (bool, error) {
						ready, err := d.completeDispatch(ctx, scheduler, metadata, result.prepared)
						if !ready {
							retryAfter = now().Add(dispatchProbeMissDelay)
						}
						return ready, err
					},
					Commit: func() {
						for _, publisher := range result.prepared.publishers {
							publisher.PublishProbe(ctx, result.prepared.request)
						}
						commit(result.prepared)
					},
				}, nil
			default:
				return nil, nil
			}
		}
		if now().Before(retryAfter) {
			return nil, nil
		}
		select {
		case d.dispatchPreparationSlots <- struct{}{}:
		default:
			return nil, nil
		}
		var once sync.Once
		release := func() { once.Do(func() { <-d.dispatchPreparationSlots }) }
		stopRequest := context.AfterFunc(ctx, release)
		stopProcessor := context.AfterFunc(processorCtx, release)
		pending = &pendingPreparation{result: make(chan preparationResult, 1), release: func() { stopRequest(); stopProcessor(); release() }}
		resultCh := pending.result
		if !producersReady {
			producers = d.newProbeDataProducers()
			producersReady = true
		}
		request := copyDispatchRequest(base)
		if request.Body != nil && tokenized != nil {
			copy := *tokenized
			request.Body.TokenizedRequest = &copy
		}
		attemptCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(processorCtx, cancel)
		hint := preferred
		go func() {
			defer cancel()
			defer stop()
			result := d.prepareProbe(attemptCtx, scheduler, request, metadata, producers, hint)
			if ctx.Err() == nil && processorCtx.Err() == nil {
				resultCh <- result
			}
		}()
		return nil, nil
	}
}

func copyDispatchRequest(base *fwksched.InferenceRequest) *fwksched.InferenceRequest {
	request := &fwksched.InferenceRequest{
		RequestID: base.RequestID, TargetModel: base.TargetModel, Headers: maps.Clone(base.Headers),
		Objectives: base.Objectives, FairnessID: base.FairnessID, RequestSizeBytes: base.RequestSizeBytes,
	}
	if base.Body != nil {
		body := *base.Body
		// Producers can update tokenization metadata without changing the immutable prompt arrays.
		if body.TokenizedRequest != nil {
			tokens := *body.TokenizedRequest
			body.TokenizedRequest = &tokens
		}
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
	return d.prepareProbe(ctx, scheduler, request, metadata, d.newProbeDataProducers(), fwkdl.ID{})
}

func (d *Director) newProbeDataProducers() []fwkrc.DataProducer {
	producers := make([]fwkrc.DataProducer, 0, len(d.requestControlPlugins.dataProducerPlugins))
	for _, producer := range d.requestControlPlugins.dataProducerPlugins {
		if factory, ok := producer.(fwkrc.ProbeDataProducerFactory); ok {
			producer = factory.NewProbeDataProducer()
		}
		producers = append(producers, producer)
	}
	return producers
}

func (d *Director) prepareProbe(ctx context.Context, scheduler dispatchScheduler, request *fwksched.InferenceRequest, metadata map[string]any, producers []fwkrc.DataProducer, preferred fwkdl.ID) preparationResult {
	candidates := d.endpointCandidates.Locate(ctx, metadata)
	var endpoints []fwksched.Endpoint
	if len(d.requestControlPlugins.screeners) == 0 {
		endpoints = d.toSchedulerEndpoints(shortlistDecoders(candidates, preferred))
	} else {
		// Screeners can depend on the full pool, including cross-role rollout coverage.
		screened := d.runScreeners(ctx, request, d.toSchedulerEndpoints(candidates))
		endpoints = shortlistDecoders(screened, preferred)
	}
	if len(endpoints) == 0 {
		return preparationResult{}
	}
	for _, producer := range producers {
		if err := dataProducerPluginsWithTimeout(ctx, producerTimeout(producer), []fwkrc.DataProducer{producer}, request, endpoints); err != nil {
			return preparationResult{}
		}
	}
	prepared := preparationResult{}
	if request.Body != nil {
		prepared.tokenized = request.Body.TokenizedRequest
	}
	endpoint, err := scheduler.ProbeDecodeLocal(ctx, request, endpoints)
	if err != nil {
		prepared.err = err
		return prepared
	}
	if endpoint == nil {
		return prepared
	}
	prepared.prepared = &preparedScheduling{request: request, endpoints: []fwksched.Endpoint{endpoint}}
	for _, producer := range producers {
		if publisher, ok := producer.(fwkrc.ProbeDataPublisher); ok {
			prepared.prepared.publishers = append(prepared.prepared.publishers, publisher)
		}
	}
	prepared.stages = []string{flowcontrol.SaturationStageDecode}
	if meta := endpoint.GetMetadata(); meta != nil {
		switch meta.Labels[bylabel.RoleLabel] {
		case bylabel.RolePrefillDecode, bylabel.RoleEncodePrefillDecode:
			prepared.reservationStages = []string{flowcontrol.SaturationStageDecode, flowcontrol.SaturationStagePrefill}
		}
	}
	return prepared
}

func shortlistDecoders[T interface {
	GetMetadata() *fwkdl.EndpointMetadata
}](candidates []T, preferred fwkdl.ID) []T {
	result := make([]T, 0, dispatchProbeCandidates)
	// A matching decoder from a prior probe keeps one slot. Other slots use reservoir sampling.
	pinned := 0
	if preferred != (fwkdl.ID{}) {
		for _, candidate := range candidates {
			if any(candidate) == nil {
				continue
			}
			meta := candidate.GetMetadata()
			if decodeCandidate(meta) && meta.ID == preferred {
				result = append(result, candidate)
				pinned = 1
				break
			}
		}
	}
	seen := 0
	for _, candidate := range candidates {
		if any(candidate) == nil {
			continue
		}
		meta := candidate.GetMetadata()
		if !decodeCandidate(meta) || (pinned != 0 && meta.ID == preferred) {
			continue
		}
		seen++
		if len(result) < dispatchProbeCandidates {
			result = append(result, candidate)
			continue
		}
		index := rand.Intn(seen) //#nosec G404 -- sampling routing candidates, not security-sensitive
		if index < dispatchProbeCandidates-pinned {
			result[pinned+index] = candidate
		}
	}
	return result
}

func decodeCandidate(meta *fwkdl.EndpointMetadata) bool {
	if meta == nil {
		return false
	}
	switch meta.Labels[bylabel.RoleLabel] {
	case "", bylabel.RoleDecode, bylabel.RolePrefillDecode, bylabel.RoleEncodePrefillDecode:
		return true
	default:
		return false
	}
}

func (d *Director) completeDispatch(ctx context.Context, scheduler dispatchScheduler, metadata map[string]any, prepared *preparedScheduling) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	candidates := d.toSchedulerEndpoints(d.endpointCandidates.Locate(ctx, metadata))
	candidates = d.runScreeners(ctx, prepared.request, candidates)
	var selected fwksched.Endpoint
	for _, candidate := range candidates {
		if sameDestination(candidate, prepared.endpoints[0]) {
			selected = candidate
			break
		}
	}
	if selected == nil {
		return false, nil
	}
	// Source-owned attributes must reflect current load. Preserve only request-produced values from the probe.
	sourceKeys := make(map[plugin.DataKey]struct{})
	for _, key := range selected.Keys() {
		sourceKeys[key] = struct{}{}
	}
	for _, key := range prepared.endpoints[0].Keys() {
		if _, present := sourceKeys[key]; present {
			continue
		}
		if value, ok := prepared.endpoints[0].Get(key); ok {
			selected.Put(key, value)
		}
	}
	prepared.endpoints = []fwksched.Endpoint{selected}
	result, err := scheduler.Schedule(ctx, prepared.request, prepared.endpoints)
	if err != nil {
		var typed errcommon.Error
		if errors.As(err, &typed) && (typed.Code == errcommon.ServiceUnavailable || typed.Code == errcommon.ResourceExhausted) {
			return false, nil
		}
		return false, err
	}
	if result.PrimaryEndpoint() == nil || scheduler.DispatchUsesPrefill(prepared.request, result) {
		return false, nil
	}
	// The full schedule must use the decoder whose cache state authorized the bypass.
	if result.PrimaryEndpoint().GetMetadata() == nil || !sameDestination(result.PrimaryEndpoint(), prepared.endpoints[0]) {
		return false, nil
	}
	prepared.result = result
	return true, nil
}

func sameDestination(a, b fwksched.Endpoint) bool {
	am, bm := a.GetMetadata(), b.GetMetadata()
	return am != nil && bm != nil && am.ID == bm.ID && am.Address == bm.Address && am.Port == bm.Port
}

func (d *Director) endpointsAvailable(ctx context.Context, metadata map[string]any, selected []fwksched.Endpoint) bool {
	candidates := d.endpointCandidates.Locate(ctx, metadata)
	for _, endpoint := range selected {
		if endpoint == nil || endpoint.GetMetadata() == nil {
			return false
		}
		wanted := endpoint.GetMetadata()
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
	return true
}
