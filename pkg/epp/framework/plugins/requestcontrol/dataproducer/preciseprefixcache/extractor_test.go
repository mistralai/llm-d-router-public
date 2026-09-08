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

package preciseprefixcache

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvevents"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
)

// Avoid a -race ding from subscriber goroutines writing through a t-bound
// logger after t.Run cleanup.
func discardCtx(t *testing.T) context.Context {
	t.Helper()
	return log.IntoContext(context.Background(), logr.Discard())
}

func newExtractorProducer(discoverPods bool) *Producer {
	cfg := kvevents.DefaultConfig()
	cfg.DiscoverPods = discoverPods
	cfg.PodDiscoveryConfig = kvevents.DefaultPodReconcilerConfig()
	cfg.PodDiscoveryConfig.SocketPort = 5557

	return &Producer{
		typedName:          plugin.TypedName{Type: PluginType, Name: PluginType},
		subscribersManager: kvevents.NewSubscriberManager(kvevents.NewPool(cfg, nil, nil, nil)),
		kvEventsConfig:     cfg,
		kvCacheIndexer:     &fakeKVCacheIndexer{index: &fakeKVBlockIndex{}},
		subscriberCtx:      context.Background(),
	}
}

func newEndpoint(name, addr string) fwkdl.Endpoint {
	return fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:      k8stypes.NamespacedName{Namespace: "ns", Name: name},
		Address: addr,
		Port:    "8080",
	}, nil)
}

func TestProducer_EndpointExtractor_InterfaceContract(t *testing.T) {
	ctx := discardCtx(t)
	p := newExtractorProducer(true)
	defer p.subscribersManager.Shutdown(ctx)

	var _ fwkdl.EndpointExtractor = p
	assert.True(t, reflect.TypeOf(p).Implements(reflect.TypeFor[fwkdl.EndpointExtractor]()))
}

func TestProducer_ExtractEndpoint_AddAndDelete(t *testing.T) {
	ctx := discardCtx(t)
	p := newExtractorProducer(true)
	defer p.subscribersManager.Shutdown(ctx)

	ep := newEndpoint("pod-a", "10.0.0.1")
	wantKey := "ns/pod-a"
	wantEndpoint := "tcp://10.0.0.1:5557"

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: ep,
	}))

	ids, endpoints := p.subscribersManager.GetActiveSubscribers()
	require.Equal(t, []string{wantKey}, ids)
	require.Equal(t, []string{wantEndpoint}, endpoints)

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: ep,
	}))
	ids, _ = p.subscribersManager.GetActiveSubscribers()
	assert.Len(t, ids, 1, "duplicate add must not create a second subscriber")

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventDelete,
		Endpoint: ep,
	}))
	ids, _ = p.subscribersManager.GetActiveSubscribers()
	assert.Empty(t, ids)
}

// DiscoverPods=false → global-socket mode, per-pod discovery off.
func TestProducer_ExtractEndpoint_DiscoverPodsDisabledIsNoOp(t *testing.T) {
	ctx := discardCtx(t)
	p := newExtractorProducer(false)
	defer p.subscribersManager.Shutdown(ctx)

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: newEndpoint("pod-a", "10.0.0.1"),
	}))

	ids, _ := p.subscribersManager.GetActiveSubscribers()
	assert.Empty(t, ids)
}

func TestProducer_ExtractEndpoint_IgnoresMissingMetadata(t *testing.T) {
	ctx := discardCtx(t)
	p := newExtractorProducer(true)
	defer p.subscribersManager.Shutdown(ctx)

	ep := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
		ID: k8stypes.NamespacedName{Namespace: "ns", Name: "pod-a"},
	}, nil)

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: ep,
	}))

	ids, _ := p.subscribersManager.GetActiveSubscribers()
	assert.Empty(t, ids)
}

// Regression: subscribers must survive request-ctx cancellation.
func TestProducer_EnsureSubscriber_SurvivesRequestCtxCancel(t *testing.T) {
	p := newExtractorProducer(true)
	defer p.subscribersManager.Shutdown(context.Background())

	reqCtx, cancel := context.WithCancel(context.Background())

	require.NoError(t, p.ensureSubscriber(reqCtx, &fwkdl.EndpointMetadata{
		ID:      k8stypes.NamespacedName{Namespace: "ns", Name: "pod-a"},
		Address: "10.0.0.1", Port: "8080",
	}))

	cancel()

	ids, _ := p.subscribersManager.GetActiveSubscribers()
	assert.ElementsMatch(t, []string{"ns/pod-a"}, ids)
}

// Per-rank subscribers at SocketPort + RankIndex (vLLM offset_endpoint_port).
func TestProducer_ExtractEndpoint_OffsetsZMQPortByRankIndex(t *testing.T) {
	ctx := discardCtx(t)
	p := newExtractorProducer(true)
	defer p.subscribersManager.Shutdown(ctx)

	endpoints := []struct {
		name    string
		address string
		rank    int
		wantZMQ string
	}{
		{name: "pod-a-rank-0", address: "10.0.0.1", rank: 0, wantZMQ: "tcp://10.0.0.1:5557"},
		{name: "pod-a-rank-1", address: "10.0.0.1", rank: 1, wantZMQ: "tcp://10.0.0.1:5558"},
		{name: "pod-a-rank-2", address: "10.0.0.1", rank: 2, wantZMQ: "tcp://10.0.0.1:5559"},
		{name: "pod-v6-rank-0", address: "fd00::1", rank: 0, wantZMQ: "tcp://[fd00::1]:5557"},
		{name: "pod-v6-rank-1", address: "fd00::1", rank: 1, wantZMQ: "tcp://[fd00::1]:5558"},
	}

	for _, ep := range endpoints {
		require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
			Type: fwkdl.EventAddOrUpdate,
			Endpoint: fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
				ID:        k8stypes.NamespacedName{Namespace: "ns", Name: ep.name},
				Address:   ep.address,
				Port:      "8080",
				RankIndex: ep.rank,
			}, nil),
		}))
	}

	ids, zmqEndpoints := p.subscribersManager.GetActiveSubscribers()
	gotByID := make(map[string]string, len(ids))
	for i, id := range ids {
		gotByID[id] = zmqEndpoints[i]
	}
	for _, ep := range endpoints {
		key := "ns/" + ep.name
		assert.Equal(t, ep.wantZMQ, gotByID[key],
			"rank %d must subscribe at SocketPort + rank", ep.rank)
	}
}

func TestProducer_EnsureSubscriber_PassesServingEndpoint(t *testing.T) {
	cfg := kvevents.DefaultConfig()
	cfg.DiscoverPods = true
	cfg.PodDiscoveryConfig = kvevents.DefaultPodReconcilerConfig()
	cfg.PodDiscoveryConfig.SocketPort = 5557

	subscribers := &fakeSubscriberManager{}
	p := &Producer{
		typedName:          plugin.TypedName{Type: PluginType, Name: PluginType},
		subscribersManager: subscribers,
		kvEventsConfig:     cfg,
		subscriberCtx:      context.Background(),
	}

	require.NoError(t, p.ensureSubscriber(context.Background(), &fwkdl.EndpointMetadata{
		ID:        k8stypes.NamespacedName{Namespace: "ns", Name: "pod-a-rank-3"},
		Address:   "10.0.0.1",
		Port:      "8003",
		RankIndex: 3,
	}))

	assert.Equal(t, []string{"ns/pod-a-rank-3"}, subscribers.ids)
	assert.Equal(t, []string{"10.0.0.1:8003"}, subscribers.sourceEndpoints)
	assert.Equal(t, []string{"tcp://10.0.0.1:5560"}, subscribers.endpoints)
}

func TestProducer_EnsureSubscriber_PassesDataParallelRank(t *testing.T) {
	cfg := kvevents.DefaultConfig()
	cfg.DiscoverPods = true
	cfg.PodDiscoveryConfig = kvevents.DefaultPodReconcilerConfig()
	cfg.PodDiscoveryConfig.SocketPort = 5557
	cfg.PodDiscoveryConfig.ReplaySocketPort = 5657

	subscribers := &fakeSubscriberManager{}
	p := &Producer{
		typedName:          plugin.TypedName{Type: PluginType, Name: PluginType},
		subscribersManager: subscribers,
		kvEventsConfig:     cfg,
		subscriberCtx:      context.Background(),
	}
	rank := 3

	require.NoError(t, p.ensureSubscriber(context.Background(), &fwkdl.EndpointMetadata{
		ID:               k8stypes.NamespacedName{Namespace: "ns", Name: "pod-a-rank-3"},
		Address:          "10.0.0.1",
		Port:             "8000",
		DataParallelRank: &rank,
	}))

	assert.Equal(t, []string{"tcp://10.0.0.1:5560"}, subscribers.endpoints)
	assert.Equal(t, []string{"tcp://10.0.0.1:5660"}, subscribers.replayEndpoints)
	require.Len(t, subscribers.dataParallelRanks, 1)
	require.NotNil(t, subscribers.dataParallelRanks[0])
	assert.Equal(t, rank, *subscribers.dataParallelRanks[0])
}

// IPv6 addresses must be bracketed in the zmq endpoint.
func TestProducer_EnsureSubscriber_IPv6BracketsEndpoint(t *testing.T) {
	cfg := kvevents.DefaultConfig()
	cfg.DiscoverPods = true
	cfg.PodDiscoveryConfig = kvevents.DefaultPodReconcilerConfig()
	cfg.PodDiscoveryConfig.SocketPort = 5557

	subscribers := &fakeSubscriberManager{}
	p := &Producer{
		typedName:          plugin.TypedName{Type: PluginType, Name: PluginType},
		subscribersManager: subscribers,
		kvEventsConfig:     cfg,
		subscriberCtx:      context.Background(),
	}

	require.NoError(t, p.ensureSubscriber(context.Background(), &fwkdl.EndpointMetadata{
		ID:        k8stypes.NamespacedName{Namespace: "ns", Name: "pod-v6"},
		Address:   "fd00::1",
		Port:      "8080",
		RankIndex: 0,
	}))

	assert.Equal(t, []string{"tcp://[fd00::1]:5557"}, subscribers.endpoints)
	assert.Equal(t, []string{"fd00::1:8080"}, subscribers.sourceEndpoints)
}

// RankIndex=0 must dial the base SocketPort unchanged.
func TestProducer_ExtractEndpoint_SingleRankUsesBaseSocketPort(t *testing.T) {
	ctx := discardCtx(t)
	p := newExtractorProducer(true)
	defer p.subscribersManager.Shutdown(ctx)

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type: fwkdl.EventAddOrUpdate,
		Endpoint: fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
			ID:      k8stypes.NamespacedName{Namespace: "ns", Name: "pod-a"},
			Address: "10.0.0.1",
			Port:    "8080",
			// RankIndex stays at its zero value.
		}, nil),
	}))

	_, zmqEndpoints := p.subscribersManager.GetActiveSubscribers()
	assert.Equal(t, []string{"tcp://10.0.0.1:5557"}, zmqEndpoints,
		"single-rank pod (RankIndex=0) must dial the base SocketPort")
}

// EventDelete clears index entries for the removed pod's address.
func TestProducer_ExtractEndpoint_DeleteClearsIndex(t *testing.T) {
	ctx := discardCtx(t)

	var clearedPod string
	fakeIndex := &fakeKVBlockIndex{
		clearFn: func(_ context.Context, podIdentifier string) error {
			clearedPod = podIdentifier
			return nil
		},
	}
	fakeIndexer := &fakeKVCacheIndexer{index: fakeIndex}

	cfg := kvevents.DefaultConfig()
	cfg.DiscoverPods = true
	cfg.PodDiscoveryConfig = kvevents.DefaultPodReconcilerConfig()
	cfg.PodDiscoveryConfig.SocketPort = 5557

	p := &Producer{
		typedName:          plugin.TypedName{Type: PluginType, Name: PluginType},
		subscribersManager: kvevents.NewSubscriberManager(kvevents.NewPool(cfg, nil, nil, nil)),
		kvEventsConfig:     cfg,
		kvCacheIndexer:     fakeIndexer,
		subscriberCtx:      context.Background(),
	}
	defer p.subscribersManager.Shutdown(ctx)

	ep := newEndpoint("pod-clear", "10.0.0.99")

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: ep,
	}))

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventDelete,
		Endpoint: ep,
	}))

	assert.Equal(t, "10.0.0.99:8080", clearedPod, "index should be cleared using pod IP:Port matching PodIdentifier format")

	ids, _ := p.subscribersManager.GetActiveSubscribers()
	assert.Empty(t, ids)
}

func TestProducer_ExtractEndpoint_DeleteClearsOnlyDataParallelRank(t *testing.T) {
	ctx := discardCtx(t)

	var clearedPod string
	var clearedRank int
	fakeIndex := &fakeKVBlockIndex{
		clearFn: func(_ context.Context, _ string) error {
			t.Fatal("rank endpoint delete must not clear all ranks")
			return nil
		},
		clearRankFn: func(_ context.Context, podIdentifier string, dataParallelRank int) error {
			clearedPod = podIdentifier
			clearedRank = dataParallelRank
			return nil
		},
	}
	cfg := kvevents.DefaultConfig()
	cfg.DiscoverPods = true
	cfg.PodDiscoveryConfig = kvevents.DefaultPodReconcilerConfig()
	p := &Producer{
		typedName:          plugin.TypedName{Type: PluginType, Name: PluginType},
		subscribersManager: kvevents.NewSubscriberManager(kvevents.NewPool(cfg, nil, nil, nil)),
		kvEventsConfig:     cfg,
		kvCacheIndexer:     &fakeKVCacheIndexer{index: fakeIndex},
		subscriberCtx:      context.Background(),
	}
	defer p.subscribersManager.Shutdown(ctx)
	rank := 2
	ep := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:               k8stypes.NamespacedName{Namespace: "ns", Name: "pod-clear-rank-2"},
		Address:          "10.0.0.99",
		Port:             "8080",
		DataParallelRank: &rank,
	}, nil)

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: ep}))
	assert.Equal(t, "10.0.0.99:8080", clearedPod)
	assert.Equal(t, rank, clearedRank)
}

// Delete by NamespacedName must work even when the event has no address.
func TestProducer_ExtractEndpoint_DeleteWithMissingAddressRemovesExistingSubscriber(t *testing.T) {
	ctx := discardCtx(t)
	p := newExtractorProducer(true)
	defer p.subscribersManager.Shutdown(ctx)

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: newEndpoint("pod-a", "10.0.0.1"),
	}))

	ids, _ := p.subscribersManager.GetActiveSubscribers()
	require.Len(t, ids, 1)

	deleteEndpoint := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
		ID: k8stypes.NamespacedName{Namespace: "ns", Name: "pod-a"},
	}, nil)

	require.NoError(t, p.Extract(ctx, fwkdl.EndpointEvent{
		Type:     fwkdl.EventDelete,
		Endpoint: deleteEndpoint,
	}))

	ids, _ = p.subscribersManager.GetActiveSubscribers()
	assert.Empty(t, ids)
}

const (
	testRankPodGroupLabel = "leaderworkerset.sigs.k8s.io/group-index"
	testRankPodRankLabel  = "leaderworkerset.sigs.k8s.io/worker-index"
)

func newRankPodMappingProducer(t *testing.T, subscribers subscriberManager, index kvblock.Index) *Producer {
	return newRankPodMappingProducerWithRanks(t, subscribers, index, 1)
}

func newRankPodMappingProducerWithRanks(t *testing.T, subscribers subscriberManager, index kvblock.Index, ranksPerPod int) *Producer {
	t.Helper()
	cfg := kvevents.DefaultConfig()
	cfg.DiscoverPods = true
	cfg.PodDiscoveryConfig.PodNamespace = "ns"
	cfg.PodDiscoveryConfig.PodLabelSelector = "app.kubernetes.io/instance=model"
	cfg.PodDiscoveryConfig.SocketPort = 5557
	cfg.PodDiscoveryConfig.ReplaySocketPort = 5657
	cfg.PodDiscoveryConfig.RankPodMapping = &kvevents.RankPodMappingConfig{
		GroupLabelKey: testRankPodGroupLabel,
		RankLabelKey:  testRankPodRankLabel,
		RanksPerPod:   ranksPerPod,
	}
	resolver, err := newRankPodResolver(cfg.PodDiscoveryConfig)
	require.NoError(t, err)
	return &Producer{
		typedName:          plugin.TypedName{Type: PluginType, Name: PluginType},
		subscribersManager: subscribers,
		kvEventsConfig:     cfg,
		kvCacheIndexer:     &fakeKVCacheIndexer{index: index},
		subscriberCtx:      context.Background(),
		rankPodResolver:    resolver,
	}
}

func rankEndpoint(group string, rank int) fwkdl.Endpoint {
	return rankEndpointAt(group, rank, "10.0.0.10")
}

func rankEndpointAt(group string, rank int, address string) fwkdl.Endpoint {
	rankValue := rank
	return fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:               k8stypes.NamespacedName{Namespace: "ns", Name: fmt.Sprintf("leader-%s-rank-%d", group, rank)},
		Name:             "leader",
		Address:          address,
		Port:             "8000",
		Labels:           map[string]string{testRankPodGroupLabel: group},
		RankIndex:        rank,
		DataParallelRank: &rankValue,
	}, nil)
}

func rankWorkerPod(t *testing.T, name, group string, worker int, ip string, ready bool) *unstructured.Unstructured {
	t.Helper()
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				"app.kubernetes.io/instance": "model",
				testRankPodGroupLabel:        group,
				testRankPodRankLabel:         strconv.Itoa(worker),
			},
		},
		Status: corev1.PodStatus{
			PodIP: ip,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: status,
			}},
		},
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	require.NoError(t, err)
	return &unstructured.Unstructured{Object: object}
}

func TestProducer_RankPodMappingUsesWorkerTransportAndLeaderIdentity(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	p := newRankPodMappingProducer(t, subscribers, &fakeKVBlockIndex{})
	podHandler := &rankPodNotificationHandler{producer: p}

	require.NoError(t, p.Extract(context.Background(), fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: rankEndpoint("7", 1),
	}))
	assert.Empty(t, subscribers.endpoints)

	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type:   fwkdl.EventAddOrUpdate,
		Object: rankWorkerPod(t, "worker-7-1", "7", 1, "10.0.0.21", true),
	}))

	require.Len(t, subscribers.endpoints, 1)
	assert.Equal(t, "10.0.0.10:8000", subscribers.sourceEndpoints[0])
	assert.Equal(t, "tcp://10.0.0.21:5558", subscribers.endpoints[0])
	assert.Equal(t, "tcp://10.0.0.21:5658", subscribers.replayEndpoints[0])
	require.NotNil(t, subscribers.dataParallelRanks[0])
	assert.Equal(t, 1, *subscribers.dataParallelRanks[0])
}

func TestProducer_RankPodMappingOffsetsPortsByGlobalRank(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	p := newRankPodMappingProducer(t, subscribers, &fakeKVBlockIndex{})
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type: fwkdl.EventAddOrUpdate, Endpoint: rankEndpoint("7", 3),
	}))
	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type:   fwkdl.EventAddOrUpdate,
		Object: rankWorkerPod(t, "worker-7-3", "7", 3, "10.0.0.23", true),
	}))

	require.Len(t, subscribers.endpoints, 1)
	assert.Equal(t, "tcp://10.0.0.23:5560", subscribers.endpoints[0])
	assert.Equal(t, "tcp://10.0.0.23:5660", subscribers.replayEndpoints[0])
}

func TestProducer_RankPodMappingHandlesPodFirstOrdering(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	p := newRankPodMappingProducer(t, subscribers, &fakeKVBlockIndex{})
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type:   fwkdl.EventAddOrUpdate,
		Object: rankWorkerPod(t, "worker-7-1", "7", 1, "10.0.0.21", true),
	}))
	assert.Empty(t, subscribers.endpoints)

	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: rankEndpoint("7", 1),
	}))

	require.Len(t, subscribers.endpoints, 1)
	assert.Equal(t, "tcp://10.0.0.21:5558", subscribers.endpoints[0])
}

func TestProducer_RankPodMappingReplacesSubscriberAfterWorkerRestart(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	var clearedPod string
	var clearedRank int
	index := &fakeKVBlockIndex{clearRankFn: func(_ context.Context, podIdentifier string, rank int) error {
		clearedPod = podIdentifier
		clearedRank = rank
		return nil
	}}
	p := newRankPodMappingProducer(t, subscribers, index)
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: rankEndpoint("7", 2),
	}))
	for _, ip := range []string{"10.0.0.22", "10.0.0.32"} {
		require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
			Type:   fwkdl.EventAddOrUpdate,
			Object: rankWorkerPod(t, "worker-7-2", "7", 2, ip, true),
		}))
	}

	require.Len(t, subscribers.endpoints, 2)
	assert.Equal(t, "tcp://10.0.0.22:5559", subscribers.endpoints[0])
	assert.Equal(t, "tcp://10.0.0.32:5559", subscribers.endpoints[1])
	assert.Equal(t, subscribers.ids[0], subscribers.ids[1])
	assert.Contains(t, subscribers.removed, "ns/leader-7-rank-2")
	assert.Equal(t, "10.0.0.10:8000", clearedPod)
	assert.Equal(t, 2, clearedRank)
}

func TestProducer_RankPodMappingDuplicateWorkerUpdateIsNoOp(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	p := newRankPodMappingProducer(t, subscribers, &fakeKVBlockIndex{})
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type: fwkdl.EventAddOrUpdate, Endpoint: rankEndpoint("7", 2),
	}))
	worker := rankWorkerPod(t, "worker-7-2", "7", 2, "10.0.0.22", true)
	for range 2 {
		require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
			Type: fwkdl.EventAddOrUpdate, Object: worker,
		}))
	}

	assert.Len(t, subscribers.endpoints, 1)
	assert.Empty(t, subscribers.removed)
}

func TestProducer_RankPodMappingClearsOnlyRestartedRank(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	var clearedPod string
	var clearedRank int
	index := &fakeKVBlockIndex{clearRankFn: func(_ context.Context, podIdentifier string, rank int) error {
		clearedPod = podIdentifier
		clearedRank = rank
		return nil
	}}
	p := newRankPodMappingProducer(t, subscribers, index)
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: rankEndpoint("7", 3),
	}))
	worker := rankWorkerPod(t, "worker-7-3", "7", 3, "10.0.0.23", true)
	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type: fwkdl.EventAddOrUpdate, Object: worker,
	}))
	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type: fwkdl.EventDelete, Object: worker,
	}))

	assert.Contains(t, subscribers.removed, "ns/leader-7-rank-3")
	assert.Equal(t, "10.0.0.10:8000", clearedPod)
	assert.Equal(t, 3, clearedRank)
}

func TestProducer_RankPodMappingEndpointDeleteUsesStoredIdentity(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	var clearedPod string
	var clearedRank int
	index := &fakeKVBlockIndex{clearRankFn: func(_ context.Context, podIdentifier string, rank int) error {
		clearedPod = podIdentifier
		clearedRank = rank
		return nil
	}}
	p := newRankPodMappingProducer(t, subscribers, index)
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	endpoint := rankEndpoint("7", 2)
	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type: fwkdl.EventAddOrUpdate, Object: rankWorkerPod(t, "worker-7-2", "7", 2, "10.0.0.22", true),
	}))
	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type: fwkdl.EventAddOrUpdate, Endpoint: endpoint,
	}))
	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type: fwkdl.EventDelete,
		Endpoint: fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
			ID: endpoint.GetMetadata().ID,
		}, nil),
	}))

	assert.Contains(t, subscribers.removed, "ns/leader-7-rank-2")
	assert.Equal(t, "10.0.0.10:8000", clearedPod)
	assert.Equal(t, 2, clearedRank)
}

func TestProducer_RankPodMappingNotReadyWorkerRemovesSubscriber(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	var clearedRank int
	index := &fakeKVBlockIndex{clearRankFn: func(_ context.Context, _ string, rank int) error {
		clearedRank = rank
		return nil
	}}
	p := newRankPodMappingProducer(t, subscribers, index)
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type: fwkdl.EventAddOrUpdate, Endpoint: rankEndpoint("7", 1),
	}))
	worker := rankWorkerPod(t, "worker-7-1", "7", 1, "10.0.0.21", true)
	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type: fwkdl.EventAddOrUpdate, Object: worker,
	}))
	pod := &corev1.Pod{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(worker.Object, pod))
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	require.NoError(t, err)
	notReady := &unstructured.Unstructured{Object: object}
	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type: fwkdl.EventAddOrUpdate, Object: notReady,
	}))

	assert.Contains(t, subscribers.removed, "ns/leader-7-rank-1")
	assert.Equal(t, 1, clearedRank)
}

func TestProducer_RankPodMappingFiltersWorkerPods(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	p := newRankPodMappingProducer(t, subscribers, &fakeKVBlockIndex{})
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}
	require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
		Type: fwkdl.EventAddOrUpdate, Endpoint: rankEndpoint("7", 1),
	}))

	wrongNamespace := rankWorkerPod(t, "wrong-ns", "7", 1, "10.0.0.31", true)
	wrongNamespace.SetNamespace("other")
	wrongSelector := rankWorkerPod(t, "wrong-selector", "7", 1, "10.0.0.32", true)
	wrongSelector.SetLabels(map[string]string{
		"app.kubernetes.io/instance": "other",
		testRankPodGroupLabel:        "7",
		testRankPodRankLabel:         "1",
	})
	notReady := rankWorkerPod(t, "not-ready", "7", 1, "10.0.0.33", false)
	missingIP := rankWorkerPod(t, "missing-ip", "7", 1, "", true)
	for _, pod := range []*unstructured.Unstructured{wrongNamespace, wrongSelector, notReady, missingIP} {
		require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
			Type: fwkdl.EventAddOrUpdate, Object: pod,
		}))
	}
	assert.Empty(t, subscribers.endpoints)
}

func TestProducer_RankPodMappingSeparatesGroups(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	p := newRankPodMappingProducer(t, subscribers, &fakeKVBlockIndex{})
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	for _, endpoint := range []fwkdl.Endpoint{
		rankEndpointAt("7", 1, "10.0.0.10"),
		rankEndpointAt("8", 1, "10.0.0.11"),
	} {
		require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
			Type: fwkdl.EventAddOrUpdate, Endpoint: endpoint,
		}))
	}
	for _, pod := range []*unstructured.Unstructured{
		rankWorkerPod(t, "worker-7-1", "7", 1, "10.0.0.21", true),
		rankWorkerPod(t, "worker-8-1", "8", 1, "10.0.0.31", true),
	} {
		require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
			Type: fwkdl.EventAddOrUpdate, Object: pod,
		}))
	}

	require.Len(t, subscribers.endpoints, 2)
	assert.ElementsMatch(t, []string{"tcp://10.0.0.21:5558", "tcp://10.0.0.31:5558"}, subscribers.endpoints)
	assert.ElementsMatch(t, []string{"10.0.0.10:8000", "10.0.0.11:8000"}, subscribers.sourceEndpoints)
}

func TestProducer_RankPodMappingSupportsMultipleRanksPerPod(t *testing.T) {
	subscribers := &fakeSubscriberManager{}
	p := newRankPodMappingProducerWithRanks(t, subscribers, &fakeKVBlockIndex{}, 2)
	endpointHandler := &rankEndpointHandler{producer: p}
	podHandler := &rankPodNotificationHandler{producer: p}

	for _, rank := range []int{2, 3} {
		require.NoError(t, endpointHandler.Extract(context.Background(), fwkdl.EndpointEvent{
			Type: fwkdl.EventAddOrUpdate, Endpoint: rankEndpoint("7", rank),
		}))
	}
	require.NoError(t, podHandler.Extract(context.Background(), fwkdl.NotificationEvent{
		Type:   fwkdl.EventAddOrUpdate,
		Object: rankWorkerPod(t, "worker-7-1", "7", 1, "10.0.0.21", true),
	}))

	assert.ElementsMatch(t, []string{"tcp://10.0.0.21:5559", "tcp://10.0.0.21:5560"}, subscribers.endpoints)
	assert.ElementsMatch(t, []string{"tcp://10.0.0.21:5659", "tcp://10.0.0.21:5660"}, subscribers.replayEndpoints)
}

type rankPodCaptureRegistrar struct {
	registrations []fwkdl.PendingRegistration
}

func (r *rankPodCaptureRegistrar) Register(registration fwkdl.PendingRegistration) error {
	r.registrations = append(r.registrations, registration)
	return nil
}

func TestProducer_RankPodMappingRegistersPodNotifications(t *testing.T) {
	p := newRankPodMappingProducer(t, &fakeSubscriberManager{}, &fakeKVBlockIndex{})
	registrar := &rankPodCaptureRegistrar{}

	require.NoError(t, p.RegisterDependencies(registrar))
	require.Len(t, registrar.registrations, 1)
	registration := registrar.registrations[0]
	assert.Equal(t, sourcenotifications.NotificationSourceType, registration.SourceType)
	handler, ok := registration.Extractor.(fwkdl.NotificationExtractor)
	require.True(t, ok)
	assert.Equal(t, podGVK, handler.GVK())
	source, ok := registration.DefaultSource.(fwkdl.NotificationSource)
	require.True(t, ok)
	assert.Equal(t, podGVK, source.GVK())
}

func TestProducer_WithoutRankPodMappingDoesNotRegisterPodNotifications(t *testing.T) {
	p := newExtractorProducer(true)
	registrar := &rankPodCaptureRegistrar{}

	require.NoError(t, p.RegisterDependencies(registrar))
	assert.Empty(t, registrar.registrations)
}

func TestRankPodMappingRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*kvevents.PodDiscoveryConfig)
		want   string
	}{
		{
			name: "missing group label",
			mutate: func(config *kvevents.PodDiscoveryConfig) {
				config.RankPodMapping.GroupLabelKey = ""
			},
			want: "groupLabelKey",
		},
		{
			name: "missing rank label",
			mutate: func(config *kvevents.PodDiscoveryConfig) {
				config.RankPodMapping.RankLabelKey = ""
			},
			want: "rankLabelKey",
		},
		{
			name: "negative ranks per pod",
			mutate: func(config *kvevents.PodDiscoveryConfig) {
				config.RankPodMapping.RanksPerPod = -1
			},
			want: "ranksPerPod",
		},
		{
			name: "invalid selector",
			mutate: func(config *kvevents.PodDiscoveryConfig) {
				config.PodLabelSelector = "invalid in ("
			},
			want: "podLabelSelector",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := kvevents.DefaultPodReconcilerConfig()
			config.RankPodMapping = &kvevents.RankPodMappingConfig{
				GroupLabelKey: testRankPodGroupLabel,
				RankLabelKey:  testRankPodRankLabel,
				RanksPerPod:   1,
			}
			test.mutate(config)
			_, err := newRankPodResolver(config)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestRankPodMappingRequiresPodDiscovery(t *testing.T) {
	config := PluginConfig{KVEventsConfig: kvevents.DefaultConfig()}
	config.KVEventsConfig.DiscoverPods = false
	config.KVEventsConfig.PodDiscoveryConfig.RankPodMapping = &kvevents.RankPodMappingConfig{
		GroupLabelKey: testRankPodGroupLabel,
		RankLabelKey:  testRankPodRankLabel,
	}

	_, err := New(context.Background(), "test", config)
	require.ErrorContains(t, err, "discoverPods must be true")
}
