// Copyright 2026 The llm-d Authors.
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
	"strconv"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// DPRankHeaderHandlerType is the registered type name of DPRankHeaderHandler.
const DPRankHeaderHandlerType = "dp-rank-header-handler"

var _ requestcontrol.PreRequest = &DPRankHeaderHandler{}

// DPRankHeaderHandlerFactory creates a DPRankHeaderHandler.
func DPRankHeaderHandlerFactory(name string, _ *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	return NewDPRankHeaderHandler().WithName(name), nil
}

// NewDPRankHeaderHandler creates a handler that pins requests to the selected
// logical data-parallel rank.
func NewDPRankHeaderHandler() *DPRankHeaderHandler {
	return &DPRankHeaderHandler{typedName: plugin.TypedName{Type: DPRankHeaderHandlerType}}
}

// DPRankHeaderHandler sets the vLLM rank header after endpoint selection.
type DPRankHeaderHandler struct {
	typedName plugin.TypedName
}

func (p *DPRankHeaderHandler) TypedName() plugin.TypedName {
	return p.typedName
}

func (p *DPRankHeaderHandler) WithName(name string) *DPRankHeaderHandler {
	p.typedName.Name = name
	return p
}

func (p *DPRankHeaderHandler) PreRequest(_ context.Context, request *scheduling.InferenceRequest,
	schedulingResult *scheduling.SchedulingResult,
) error {
	if request == nil {
		return nil
	}
	if request.Headers == nil {
		request.Headers = make(map[string]string)
	}
	delete(request.Headers, routing.DataParallelRankHeader)

	if schedulingResult == nil {
		return nil
	}
	profileResult := schedulingResult.ProfileResults[schedulingResult.PrimaryProfileName]
	if profileResult == nil || len(profileResult.TargetEndpoints) == 0 {
		return nil
	}
	metadata := profileResult.TargetEndpoints[0].GetMetadata()
	if metadata == nil || metadata.DataParallelRank == nil || *metadata.DataParallelRank < 0 {
		return nil
	}
	request.Headers[routing.DataParallelRankHeader] = strconv.Itoa(*metadata.DataParallelRank)
	return nil
}
