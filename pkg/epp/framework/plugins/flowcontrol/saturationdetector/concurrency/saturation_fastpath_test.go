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
// Package concurrency implements a synchronous saturation detector and scheduling filter for LLM
// routing. It consumes in-flight requests and tokens data from the Endpoint's AttributeMap
// to provide instantaneous backpressure and protect endpoints from sudden traffic bursts.
//
// For detailed architectural trade-offs and configuration, see the package README.
package concurrency

import (
	"container/heap"
	"strconv"
	"testing"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	"github.com/stretchr/testify/require"
)

func TestLeastLoadedFastPathMatchesReservationAlgorithm(t *testing.T) {
	reg := newLocalRegistry()
	reg.update(fullEndpointName("a"), func(load *attrconcurrency.InFlightLoad) { load.Requests = 2; load.Tokens = 10 })
	reg.update(fullEndpointName("b"), func(load *attrconcurrency.InFlightLoad) { load.Requests = 20; load.Tokens = 200 })
	for _, mode := range []concurrencyMode{modeRequests, modeTokens, modeHybrid} {
		d := newDetector("fast-path", config{mode: mode, maxConcurrency: 10, maxTokenConcurrency: 100}, logr.Discard())
		for _, endpoints := range [][]datalayer.Endpoint{nil, {nil}, {newFakeEndpoint(reg, "a")}, {newFakeEndpoint(reg, "a"), nil, newFakeEndpoint(reg, "b")}} {
			for _, pending := range []int64{0, 1, 5} {
				require.Equal(t, d.referenceLeastLoadedSaturation(endpoints, pending), d.leastLoadedSaturation(endpoints, pending))
			}
		}
	}
}

func BenchmarkLeastLoadedWithoutReservations(b *testing.B) {
	for _, count := range []int{16, 256} {
		reg := newLocalRegistry()
		endpoints := make([]datalayer.Endpoint, count)
		for i := range endpoints {
			endpoints[i] = newFakeEndpoint(reg, strconv.Itoa(i))
		}
		d := newDetector("benchmark", config{mode: modeRequests, maxConcurrency: 10}, logr.Discard())
		b.Run(strconv.Itoa(count)+"/heap", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				d.referenceLeastLoadedSaturation(endpoints, 0)
			}
		})
		b.Run(strconv.Itoa(count)+"/scan", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				d.leastLoadedSaturation(endpoints, 0)
			}
		})
	}
}

func (d *detector) referenceLeastLoadedSaturation(endpoints []datalayer.Endpoint, pending int64) float64 {
	loads := make(endpointLoadHeap, 0, len(endpoints))
	for _, e := range endpoints {
		if e == nil {
			continue
		}
		load := &attrconcurrency.InFlightLoad{}
		if e.GetMetadata() != nil {
			load = d.getLoad(e.GetAttributes())
		}
		loads = append(loads, endpointLoad{
			requests:   load.Requests,
			tokens:     load.Tokens,
			saturation: d.endpointSaturation(load.Requests, load.Tokens),
		})
	}
	if len(loads) == 0 {
		return 1.0
	}

	heap.Init(&loads)
	if d.config.mode != modeTokens {
		for range pending {
			loads[0].requests++
			loads[0].saturation = d.endpointSaturation(loads[0].requests, loads[0].tokens)
			heap.Fix(&loads, 0)
		}
	}
	return loads[0].saturation
}
