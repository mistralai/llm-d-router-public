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

package prefixcacheaffinity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	attrlatency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/latency"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/maxscore"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/tokenload"
	"github.com/llm-d/llm-d-router/pkg/epp/scheduling"
)

func noMatchFilter(t *testing.T, params string) *Plugin {
	t.Helper()
	p, err := Factory("affinity", fwkplugin.StrictDecoder([]byte(params)), nil)
	require.NoError(t, err)
	return p.(*Plugin)
}

func activeEndpoint(name string, match int, requests, tokens int64) fwksched.Endpoint {
	ep := makeEndpoint(name, match, 100, tokens)
	ep.Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{Requests: requests, Tokens: tokens})
	return ep
}

func TestFilterLoadBalanceNoMatch(t *testing.T) {
	for _, tt := range []struct {
		name     string
		params   string
		matches  [2]int
		requests [2]int64
		want     []string
	}{
		{"omitted", `{}`, [2]int{0, 0}, [2]int64{1, 8}, []string{"a", "b"}},
		{"disabled", `{"enableLoadBalanceNoMatch":false}`, [2]int{0, 0}, [2]int64{1, 8}, []string{"a", "b"}},
		{"fewest requests beats lower token load", `{"enableLoadBalanceNoMatch":true}`, [2]int{0, 0}, [2]int64{1, 8}, []string{"a"}},
		{"later endpoint has fewest requests", `{"enableLoadBalanceNoMatch":true}`, [2]int{0, 0}, [2]int64{8, 1}, []string{"b"}},
		{"partial matches below threshold", `{"enableLoadBalanceNoMatch":true}`, [2]int{79, 10}, [2]int64{1, 8}, []string{"a"}},
		{"tied requests keep both", `{"enableLoadBalanceNoMatch":true}`, [2]int{0, 0}, [2]int64{2, 2}, []string{"a", "b"}},
		{"all idle keep both", `{"enableLoadBalanceNoMatch":true}`, [2]int{0, 0}, [2]int64{0, 0}, []string{"a", "b"}},
		{"cache threshold inclusive", `{"enableLoadBalanceNoMatch":true}`, [2]int{0, 80}, [2]int64{1, 8}, []string{"b"}},
		{"cache matched candidates unchanged", `{"enableLoadBalanceNoMatch":true}`, [2]int{90, 90}, [2]int64{1, 8}, []string{"a", "b"}},
		{"load override unchanged", `{"enableLoadBalanceNoMatch":true,"maxTTFTPenaltyMs":1}`, [2]int{90, 0}, [2]int64{1, 8}, []string{"a", "b"}},
		{"exploration unchanged", `{"enableLoadBalanceNoMatch":true,"explorationProbability":1}`, [2]int{0, 0}, [2]int64{1, 8}, []string{"a", "b"}},
		{"disabled threshold unchanged", `{"enableLoadBalanceNoMatch":true,"affinityThreshold":0}`, [2]int{0, 0}, [2]int64{1, 8}, []string{"a", "b"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := noMatchFilter(t, tt.params)
			endpoints := []fwksched.Endpoint{
				activeEndpoint("a", tt.matches[0], tt.requests[0], 2000),
				activeEndpoint("b", tt.matches[1], tt.requests[1], 100),
			}
			original := append([]fwksched.Endpoint(nil), endpoints...)
			result := p.Filter(context.Background(), &fwksched.InferenceRequest{}, endpoints)
			var names []string
			for _, ep := range result {
				names = append(names, ep.GetMetadata().ID.Name)
			}
			require.Equal(t, tt.want, names)
			require.Equal(t, original, endpoints, "filter must not overwrite the input candidate slice")
		})
	}
}

func TestFilterLoadBalanceNoMatchTrivialCandidates(t *testing.T) {
	p := noMatchFilter(t, `{"enableLoadBalanceNoMatch":true}`)
	require.Empty(t, p.Filter(context.Background(), nil, nil))
	endpoints := []fwksched.Endpoint{activeEndpoint("only", 0, 8, 100)}
	require.Equal(t, endpoints, p.Filter(context.Background(), nil, endpoints))
}

func TestFilterLoadBalanceNoMatchNamedProducerWithoutTTFTGate(t *testing.T) {
	p := noMatchFilter(t, `{"enableLoadBalanceNoMatch":true,"maxTTFTPenaltyMs":0,"inFlightLoadProducerName":"shared-load"}`)
	key := attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName("shared-load")
	require.Contains(t, p.Consumes().Required, key)
	require.NotContains(t, p.Consumes().Required, attrconcurrency.UncachedRequestTokensDataKey.WithNonEmptyProducerName("shared-load"))
	a, b := activeEndpoint("a", 0, 1, 2000), activeEndpoint("b", 0, 8, 100)
	// Named load disagrees with the default producer; use the configured source.
	a.Put(key, &attrconcurrency.InFlightLoad{Requests: 8})
	b.Put(key, &attrconcurrency.InFlightLoad{Requests: 1})
	require.Equal(t, []fwksched.Endpoint{b}, p.Filter(context.Background(), nil, []fwksched.Endpoint{a, b}))
}

func TestFilterLoadBalanceNoMatchWithLatencyPredictor(t *testing.T) {
	p := noMatchFilter(t, `{"enableLoadBalanceNoMatch":true,"ttftSource":"latencyPredictor","inFlightLoadProducerName":"shared-load","latencyPredictionInfoProducerName":"latency"}`)
	key := attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName("shared-load")
	require.Contains(t, p.Consumes().Required, key)
	require.Contains(t, p.Consumes().Required, attrlatency.LatencyPredictionInfoDataKey.WithNonEmptyProducerName("latency"))

	a, b := activeEndpoint("a", 0, 1, 2000), activeEndpoint("b", 0, 8, 100)
	a.Put(key, &attrconcurrency.InFlightLoad{Requests: 8})
	b.Put(key, &attrconcurrency.InFlightLoad{Requests: 1})
	require.Equal(t, []fwksched.Endpoint{b}, p.Filter(context.Background(), nil, []fwksched.Endpoint{a, b}))
}

func TestFilterLoadBalanceNoMatchDisabledDependencies(t *testing.T) {
	for _, params := range []string{
		`{"maxTTFTPenaltyMs":0}`,
		`{"enableLoadBalanceNoMatch":false,"ttftSource":"latencyPredictor"}`,
	} {
		p := noMatchFilter(t, params)
		require.NotContains(t, p.Consumes().Required, attrconcurrency.InFlightLoadDataKey)
	}
}

func TestFilterLoadBalanceNoMatchMissingTTFTSignalKeepsAll(t *testing.T) {
	p := noMatchFilter(t, `{"enableLoadBalanceNoMatch":true,"ttftSource":"latencyPredictor"}`)
	a := activeEndpoint("a", 90, 8, 2000)
	b := makeEndpoint("b", 0, -1, 100)
	endpoints := []fwksched.Endpoint{a, b}
	require.Equal(t, endpoints, p.Filter(context.Background(), nil, endpoints))
}

func TestNoMatchSchedulerUsesTokenLoadToBreakActiveRequestTies(t *testing.T) {
	filter := noMatchFilter(t, `{"enableLoadBalanceNoMatch":true,"maxTTFTPenaltyMs":0}`)
	tokenScorer, err := tokenload.TokenLoadScorerFactory("token-load", nil, nil)
	require.NoError(t, err)
	picker, err := maxscore.MaxScorePickerFactory("picker", nil, nil)
	require.NoError(t, err)
	datalayer.RegisterScopeSpecs([]fwkplugin.Plugin{filter, tokenScorer, picker})
	profile := scheduling.NewSchedulerProfile().
		WithFilters(filter).
		WithScorers(scheduling.NewWeightedScorer(tokenScorer.(fwksched.Scorer), 1)).
		WithPicker(picker.(fwksched.Picker))
	result, err := profile.Run(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{
		activeEndpoint("a", 0, 2, 2000),
		activeEndpoint("b", 0, 2, 100),
		activeEndpoint("c", 0, 10, 0),
	})
	require.NoError(t, err)
	require.Len(t, result.ScoredCandidates, 2, "the busy endpoint must be removed before token scoring")
	require.Len(t, result.TargetEndpoints, 1)
	require.Equal(t, "b", result.TargetEndpoints[0].GetMetadata().ID.Name)
}
