/*
Copyright 2026 The Kubernetes Authors.
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

package approximateprefix

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/stretchr/testify/require"
)

func TestProbeStateIsRequestLocalUntilPublication(t *testing.T) {
	producer, err := newDataProducer(t.Context(), "probe-test", defaultConfig, testHandle())
	require.NoError(t, err)
	probe := producer.NewProbeDataProducer().(*probeProducer)
	datalayer.RegisterScopeSpecs([]plugin.Plugin{producer})
	pods, endpoints := benchPods(1)
	request := &fwksched.InferenceRequest{RequestID: "probe", TargetModel: "model", Body: tokenizedBody(benchTokens(1, 128))}
	scopedRequest, scopedEndpoints, violations := datalayer.ScopeInvocation(logr.Discard(), requestcontrol.DataProducerExtensionPoint, probe, request, endpoints)
	require.NoError(t, probe.Produce(t.Context(), scopedRequest, scopedEndpoints))
	require.NoError(t, violations.Write())
	_, err = producer.pluginState.Read(request.RequestID, plugin.StateKey(producer.typedName.Name))
	require.Error(t, err)
	firstHashes := probe.hashes
	require.NotEmpty(t, firstHashes[0])
	producer.indexerInst.Add(firstHashes[0], pods[0])
	require.NoError(t, probe.Produce(t.Context(), scopedRequest, scopedEndpoints))
	require.Same(t, &firstHashes[0][0], &probe.hashes[0][0])
	value, ok := endpoints[0].Get(producer.dk)
	require.True(t, ok)
	require.Positive(t, value.(*attrprefix.PrefixCacheMatchInfo).CachedBlockCount())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	probe.PublishProbe(ctx, request)
	_, retained := fwksched.ReadRequestAttribute[*SchedulingContextState](request, producer.probeStateKey())
	require.False(t, retained)
	_, err = producer.pluginState.Read(request.RequestID, plugin.StateKey(producer.typedName.Name))
	require.NoError(t, err)
	cancel()
	require.Eventually(t, func() bool {
		_, err := producer.pluginState.Read(request.RequestID, plugin.StateKey(producer.typedName.Name))
		return err != nil
	}, time.Second, time.Millisecond)
}

func TestProbeHashCacheIncludesSaltAndModel(t *testing.T) {
	producer, err := newDataProducer(t.Context(), "probe-cache", defaultConfig, testHandle())
	require.NoError(t, err)
	probe := producer.NewProbeDataProducer().(*probeProducer)
	request := &fwksched.InferenceRequest{TargetModel: "a", Body: tokenizedBody(benchTokens(1, 128))}
	first, _ := probe.cachedHashes(t.Context(), request, 64)
	copy := *request.Body.TokenizedRequest
	request.Body.TokenizedRequest = &copy
	same, _ := probe.cachedHashes(t.Context(), request, 64)
	require.Same(t, &first[0][0], &same[0][0])
	request.Body.TokenizedRequest.CacheSalt = "salt"
	salted, _ := probe.cachedHashes(t.Context(), request, 64)
	require.NotEqual(t, first, salted)
	request.TargetModel = "b"
	other, _ := probe.cachedHashes(t.Context(), request, 64)
	require.NotEqual(t, salted, other)
	request.Body.SkipPrefixCacheMatching = true
	skipped, _ := probe.cachedHashes(t.Context(), request, 64)
	require.Empty(t, skipped)
}
