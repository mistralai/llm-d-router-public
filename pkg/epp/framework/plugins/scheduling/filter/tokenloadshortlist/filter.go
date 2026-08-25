/*
Copyright 2026 The Kubernetes Authors.

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

// Package tokenloadshortlist filters endpoints by EPP-tracked in-flight token load.
package tokenloadshortlist

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

const FilterType = "token-load-shortlist-filter"

type parameters struct {
	// MaxCandidates is the target number of endpoints retained when every
	// endpoint has a non-zero token load. Endpoints tied at the cutoff are all
	// retained, so the returned count can exceed MaxCandidates.
	MaxCandidates int `json:"maxCandidates"`

	InFlightLoadProducerName string `json:"inFlightLoadProducerName,omitempty"`
}

var _ fwksched.Filter = &Filter{}
var _ fwkplugin.ConsumerPlugin = &Filter{}

// Factory creates a token-load shortlist filter.
func Factory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	params := parameters{}
	if rawParameters != nil {
		if err := rawParameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' filter - %w", FilterType, err)
		}
	}
	if params.MaxCandidates <= 0 {
		return nil, fmt.Errorf("%s requires maxCandidates greater than zero, got %d", FilterType, params.MaxCandidates)
	}
	if name == "" {
		name = FilterType
	}

	return &Filter{
		typedName:           fwkplugin.TypedName{Type: FilterType, Name: name},
		maxCandidates:       params.MaxCandidates,
		inFlightLoadDataKey: attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName(params.InFlightLoadProducerName),
	}, nil
}

// Filter prefers unused capacity before restricting busy endpoints to the
// lowest token-load ranks.
type Filter struct {
	typedName           fwkplugin.TypedName
	maxCandidates       int
	inFlightLoadDataKey fwkplugin.DataKey
}

func (f *Filter) TypedName() fwkplugin.TypedName {
	return f.typedName
}

func (f *Filter) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Required: map[fwkplugin.DataKey]any{f.inFlightLoadDataKey: attrconcurrency.InFlightLoad{}},
	}
}

// Filter keeps every endpoint with exactly zero in-flight tokens when one is
// available. Otherwise it keeps the MaxCandidates lowest-load endpoints and
// every endpoint tied with the last retained endpoint. Endpoints with missing,
// malformed, or negative load are excluded when any valid load is available.
// When no endpoint has valid load data, the filter returns all candidates.
func (f *Filter) Filter(ctx context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	empty := make([]fwksched.Endpoint, 0, len(endpoints))
	validLoads := make([]int64, 0, len(endpoints))
	loads := make(map[fwksched.Endpoint]int64, len(endpoints))

	for _, endpoint := range endpoints {
		raw, ok := endpoint.Get(f.inFlightLoadDataKey)
		if !ok {
			continue
		}
		load, ok := raw.(*attrconcurrency.InFlightLoad)
		if !ok || load == nil || load.Tokens < 0 {
			continue
		}

		loads[endpoint] = load.Tokens
		validLoads = append(validLoads, load.Tokens)
		if load.Tokens == 0 {
			empty = append(empty, endpoint)
		}
	}

	logger := log.FromContext(ctx).V(logutil.TRACE)
	if len(empty) > 0 {
		logger.Info("Filtered endpoints to empty token-load tier", "candidates", len(endpoints), "kept", len(empty))
		return empty
	}
	if len(validLoads) == 0 {
		logger.Info("Token-load shortlist has no valid load data, keeping all endpoints", "candidates", len(endpoints))
		return endpoints
	}
	if len(validLoads) <= f.maxCandidates {
		return endpointsWithValidLoads(endpoints, loads, slices.Max(validLoads))
	}

	slices.Sort(validLoads)
	cutoff := validLoads[f.maxCandidates-1]
	filtered := endpointsWithValidLoads(endpoints, loads, cutoff)
	logger.Info("Filtered endpoints to lowest token-load tier", "candidates", len(endpoints), "kept", len(filtered), "cutoffTokens", cutoff)
	return filtered
}

func endpointsWithValidLoads(endpoints []fwksched.Endpoint, loads map[fwksched.Endpoint]int64, cutoff int64) []fwksched.Endpoint {
	filtered := make([]fwksched.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if tokens, ok := loads[endpoint]; ok && tokens <= cutoff {
			filtered = append(filtered, endpoint)
		}
	}
	return filtered
}
