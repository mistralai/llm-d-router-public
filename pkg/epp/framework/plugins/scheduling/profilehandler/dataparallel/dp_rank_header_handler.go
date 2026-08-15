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
func DPRankHeaderHandlerFactory(name string, _ *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	return NewDPRankHeaderHandler().WithName(name), nil
}

// NewDPRankHeaderHandler initializes a new DPRankHeaderHandler and returns its pointer.
func NewDPRankHeaderHandler() *DPRankHeaderHandler {
	return &DPRankHeaderHandler{typedName: plugin.TypedName{Type: DPRankHeaderHandlerType}}
}

// DPRankHeaderHandler pins a request to the data-parallel rank that holds the
// best prefix match on the endpoint scheduling selected.
//
// It exists for vLLM's Internal and Hybrid LB modes, where every rank sits
// behind one shared HTTP port and there is no rank-specific endpoint to route
// to. The x-data-parallel-rank header is the only way to bypass vLLM's internal
// queue-based balancing and reach the rank that already holds the KV blocks.
//
// It is a no-op under External LB, where each rank is its own endpoint on its
// own port and the scheduler addresses it directly: no rank is recorded during
// scoring, so no header is emitted.
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

// PreRequest translates the winning ranks recorded during scoring into an
// x-data-parallel-rank header for the selected endpoint.
func (p *DPRankHeaderHandler) PreRequest(_ context.Context, request *scheduling.InferenceRequest,
	schedulingResult *scheduling.SchedulingResult,
) error {
	if request == nil {
		return nil
	}
	if request.Headers == nil {
		request.Headers = make(map[string]string)
	}
	// A client cannot choose a rank and bypass cache-aware placement.
	delete(request.Headers, routing.DataParallelRankHeader)

	if schedulingResult == nil {
		return nil
	}
	ranks, present := scheduling.ReadRequestAttribute[map[string]int](
		request, preciseprefixcacheconstants.WinningRanksDataKey)
	if !present {
		return nil
	}

	endpoint := selectedEndpoint(schedulingResult)
	if endpoint == nil {
		return nil
	}

	// Built the same way the precise prefix cache producer builds its scoring
	// key, so the two agree; a mismatch here would silently leave the request
	// unpinned and let vLLM pick a rank by queue depth instead.
	address := fmt.Sprintf("%s:%s", endpoint.Address, endpoint.Port)
	rank, found := ranks[address]
	if !found || rank < 0 {
		return nil
	}

	request.Headers[routing.DataParallelRankHeader] = strconv.Itoa(rank)
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
