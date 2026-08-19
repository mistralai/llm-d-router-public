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

package dataparallel

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

const (
	routingDecisionPreciseKV    = "precise_kv"
	routingDecisionEndpoint     = "endpoint"
	routingDecisionVLLMInternal = "vllm_internal"
	noDataParallelRankLabel     = "none"
)

var dpRankRoutingTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "dp_rank_routing_total",
		Help: metricsutil.HelpMsgWithStability(
			"Requests routed to a logical or precise-cache-selected data-parallel rank, or delegated to the model server's internal router.",
			compbasemetrics.ALPHA,
		),
	},
	[]string{"plugin_type", "plugin_name", "decision", "rank"},
)

func registerDPRankMetrics(registerer prometheus.Registerer) error {
	if registerer == nil {
		return nil
	}
	if err := registerer.Register(dpRankRoutingTotal); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) && alreadyRegistered.ExistingCollector == dpRankRoutingTotal {
			return nil
		}
		return fmt.Errorf("register data-parallel rank routing metric: %w", err)
	}
	return nil
}

func recordDPRankRoutingDecision(pluginName, decision, rank string) {
	dpRankRoutingTotal.WithLabelValues(DPRankHeaderHandlerType, pluginName, decision, rank).Inc()
}
