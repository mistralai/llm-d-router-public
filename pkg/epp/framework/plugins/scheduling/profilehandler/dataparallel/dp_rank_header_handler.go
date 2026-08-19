// Copyright 2025 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataparallel

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	preciseprefixcacheconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/preciseprefixcache/constants"
)

// DPRankHeaderHandlerType is the type of the DPRankHeaderHandler plugin.
const DPRankHeaderHandlerType = "dp-rank-header-handler"

// compile-time type assertion
var _ requestcontrol.PreRequest = &DPRankHeaderHandler{}

// DPRankHeaderHandlerFactory defines the factory function for the DPRankHeaderHandler.
func DPRankHeaderHandlerFactory(name string, _ *json.Decoder, handle plugin.Handle) (plugin.Plugin, error) {
	if handle != nil {
		if err := registerDPRankMetrics(handle.Metrics()); err != nil {
			return nil, err
		}
	}
	return NewDPRankHeaderHandler().WithName(name), nil
}

// NewDPRankHeaderHandler initializes a new DPRankHeaderHandler and returns its pointer.
func NewDPRankHeaderHandler() *DPRankHeaderHandler {
	return &DPRankHeaderHandler{typedName: plugin.TypedName{Type: DPRankHeaderHandlerType}}
}

// DPRankHeaderHandler pins a request to the selected logical data-parallel
// endpoint's rank.
//
// It exists for vLLM's Internal and Hybrid LB modes, where every rank sits
// behind one shared HTTP port and there is no rank-specific endpoint to route
// to. The x-data-parallel-rank header is the only way to bypass vLLM's internal
// queue-based balancing and reach the rank that already holds the KV blocks.
//
// Legacy shared-port discovery can also supply a precise-cache winning rank
// through request attributes. Under External LB, each rank is addressed by a
// distinct network endpoint and carries no DataParallelRank metadata.
type DPRankHeaderHandler struct {
	typedName plugin.TypedName
}

// TypedName returns the typed name of the plugin.
func (p *DPRankHeaderHandler) TypedName() plugin.TypedName {
	return p.typedName
}

// WithName sets the name of the plugin.
func (p *DPRankHeaderHandler) WithName(name string) *DPRankHeaderHandler {
	p.typedName.Name = name
	return p
}

// PreRequest writes the selected logical endpoint's rank, or the legacy
// precise-cache winning rank, to x-data-parallel-rank.
func (p *DPRankHeaderHandler) PreRequest(_ context.Context, request *scheduling.InferenceRequest,
	schedulingResult *scheduling.SchedulingResult,
) error {
	if request == nil {
		return nil
	}
	if request.Headers == nil {
		request.Headers = make(map[string]string)
	}
	// A client cannot choose a rank and bypass EPP placement.
	delete(request.Headers, routing.DataParallelRankHeader)

	if schedulingResult == nil {
		return nil
	}
	endpoint := selectedEndpoint(schedulingResult)
	if endpoint == nil {
		return nil
	}
	if endpoint.DataParallelRank != nil && *endpoint.DataParallelRank >= 0 {
		rank := *endpoint.DataParallelRank
		request.Headers[routing.DataParallelRankHeader] = strconv.Itoa(rank)
		recordDPRankRoutingDecision(p.typedName.Name, routingDecisionEndpoint, strconv.Itoa(rank))
		return nil
	}
	ranks, present := scheduling.ReadRequestAttribute[map[string]int](
		request, preciseprefixcacheconstants.WinningRanksDataKey)
	if !present {
		recordDPRankRoutingDecision(p.typedName.Name, routingDecisionVLLMInternal, noDataParallelRankLabel)
		return nil
	}

	// Built the same way the precise prefix cache producer builds its scoring
	// key, so the two agree; a mismatch here would silently leave the request
	// unpinned and let vLLM pick a rank by queue depth instead.
	address := fmt.Sprintf("%s:%s", endpoint.Address, endpoint.Port)
	rank, found := ranks[address]
	if !found || rank < 0 {
		recordDPRankRoutingDecision(p.typedName.Name, routingDecisionVLLMInternal, noDataParallelRankLabel)
		return nil
	}

	request.Headers[routing.DataParallelRankHeader] = strconv.Itoa(rank)
	recordDPRankRoutingDecision(p.typedName.Name, routingDecisionPreciseKV, strconv.Itoa(rank))
	return nil
}

// selectedEndpoint returns the metadata of the endpoint the request will be
// sent to, or nil when scheduling produced none.
func selectedEndpoint(schedulingResult *scheduling.SchedulingResult) *fwkdl.EndpointMetadata {
	profileResult := schedulingResult.ProfileResults[schedulingResult.PrimaryProfileName]
	if profileResult == nil || len(profileResult.TargetEndpoints) == 0 {
		return nil
	}
	return profileResult.TargetEndpoints[0].GetMetadata()
}
