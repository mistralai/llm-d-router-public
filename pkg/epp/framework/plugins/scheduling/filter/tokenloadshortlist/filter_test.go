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

package tokenloadshortlist

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

func endpoint(t *testing.T, name string, tokens *int64) fwksched.Endpoint {
	t.Helper()
	meta := &fwkdl.EndpointMetadata{ID: types.NamespacedName{Name: name, Namespace: "default"}}
	ep := fwksched.NewEndpoint(meta, &fwkdl.Metrics{}, fwkdl.NewAttributes())
	if tokens != nil {
		ep.Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{Tokens: *tokens})
	}
	return ep
}

func tokenCount(tokens int64) *int64 {
	return &tokens
}

func names(endpoints []fwksched.Endpoint) []string {
	result := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		result = append(result, endpoint.GetMetadata().ID.Name)
	}
	return result
}

func testFilter(maxCandidates int) *Filter {
	return &Filter{
		typedName:           fwkplugin.TypedName{Type: FilterType, Name: "test"},
		maxCandidates:       maxCandidates,
		inFlightLoadDataKey: attrconcurrency.InFlightLoadDataKey,
	}
}

func TestFilter_KeepsAllEmptyEndpoints(t *testing.T) {
	endpoints := []fwksched.Endpoint{
		endpoint(t, "busy-a", tokenCount(10)),
		endpoint(t, "empty-a", tokenCount(0)),
		endpoint(t, "empty-b", tokenCount(0)),
		endpoint(t, "busy-b", tokenCount(1)),
	}

	got := testFilter(1).Filter(context.Background(), nil, endpoints)

	assert.Equal(t, []string{"empty-a", "empty-b"}, names(got))
}

func TestFilter_KeepsLowestLoads(t *testing.T) {
	endpoints := []fwksched.Endpoint{
		endpoint(t, "high", tokenCount(30)),
		endpoint(t, "lowest", tokenCount(10)),
		endpoint(t, "middle", tokenCount(20)),
	}

	got := testFilter(2).Filter(context.Background(), nil, endpoints)

	assert.Equal(t, []string{"lowest", "middle"}, names(got))
}

func TestFilter_KeepsCutoffTies(t *testing.T) {
	endpoints := []fwksched.Endpoint{
		endpoint(t, "cutoff-a", tokenCount(20)),
		endpoint(t, "lowest", tokenCount(10)),
		endpoint(t, "high", tokenCount(30)),
		endpoint(t, "cutoff-b", tokenCount(20)),
	}

	got := testFilter(2).Filter(context.Background(), nil, endpoints)

	assert.Equal(t, []string{"cutoff-a", "lowest", "cutoff-b"}, names(got))
}

func TestFilter_UnknownLoads(t *testing.T) {
	t.Run("excluded when valid loads exist", func(t *testing.T) {
		endpoints := []fwksched.Endpoint{
			endpoint(t, "unknown", nil),
			endpoint(t, "valid", tokenCount(10)),
			endpoint(t, "negative", tokenCount(-1)),
		}

		got := testFilter(2).Filter(context.Background(), nil, endpoints)

		assert.Equal(t, []string{"valid"}, names(got))
	})

	t.Run("fail open when all loads are unknown", func(t *testing.T) {
		endpoints := []fwksched.Endpoint{
			endpoint(t, "unknown-a", nil),
			endpoint(t, "unknown-b", tokenCount(-1)),
		}

		got := testFilter(1).Filter(context.Background(), nil, endpoints)

		assert.Equal(t, endpoints, got)
	})
}

func TestFactory(t *testing.T) {
	plugin, err := Factory("shortlist", fwkplugin.StrictDecoder([]byte(`{"maxCandidates": 4}`)), nil)
	require.NoError(t, err)

	filter, ok := plugin.(*Filter)
	require.True(t, ok)
	assert.Equal(t, 4, filter.maxCandidates)
	assert.Equal(t, fwkplugin.TypedName{Type: FilterType, Name: "shortlist"}, filter.TypedName())
}

func TestFactory_RejectsInvalidMaxCandidates(t *testing.T) {
	for _, config := range [][]byte{nil, []byte(`{"maxCandidates": 0}`), []byte(`{"maxCandidates": -1}`)} {
		_, err := Factory("shortlist", fwkplugin.StrictDecoder(config), nil)
		assert.Error(t, err)
	}
}

func TestFilter_ConsumesInFlightLoad(t *testing.T) {
	filter := testFilter(2)

	consumes := filter.Consumes()

	require.Len(t, consumes.Required, 1)
	assert.Equal(t, attrconcurrency.InFlightLoad{}, consumes.Required[attrconcurrency.InFlightLoadDataKey])
}
