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
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
	podutil "github.com/llm-d/llm-d-router/pkg/epp/util/pod"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvevents"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const rankPodExtractorType = "precise-prefix-cache-rank-pod-extractor"

var podGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

var (
	_ fwkdl.Registrant            = (*Producer)(nil)
	_ fwkdl.EndpointExtractor     = (*rankEndpointHandler)(nil)
	_ fwkdl.NotificationExtractor = (*rankPodNotificationHandler)(nil)
)

type rankPodKey struct {
	namespace string
	group     string
	rank      int
}

type rankEndpointState struct {
	key      rankPodKey
	metadata *fwkdl.EndpointMetadata
}

type rankWorkerState struct {
	pod types.NamespacedName
	ip  string
}

type rankSubscription struct {
	id                types.NamespacedName
	sourceEndpoint    string
	transportEndpoint string
	replayEndpoint    string
	rank              int
}

func (s *rankSubscription) equal(other *rankSubscription) bool {
	if s == nil || other == nil {
		return s == other
	}
	return s.id == other.id &&
		s.sourceEndpoint == other.sourceEndpoint &&
		s.transportEndpoint == other.transportEndpoint &&
		s.replayEndpoint == other.replayEndpoint &&
		s.rank == other.rank
}

type rankPodResolver struct {
	selector      labels.Selector
	namespace     string
	groupLabelKey string
	rankLabelKey  string
	ranksPerPod   int
	socketPort    int
	replayPort    int

	mu              sync.Mutex
	endpoints       map[types.NamespacedName]rankEndpointState
	endpointsByRank map[rankPodKey]map[types.NamespacedName]struct{}
	workers         map[rankPodKey]rankWorkerState
	workerRanks     map[types.NamespacedName][]rankPodKey
	applied         map[types.NamespacedName]rankSubscription
	versions        map[types.NamespacedName]uint64
}

func newRankPodResolver(config *kvevents.PodDiscoveryConfig) (*rankPodResolver, error) {
	if config == nil || config.RankPodMapping == nil {
		return nil, errors.New("rankPodMapping configuration is required")
	}
	mapping := config.RankPodMapping
	if mapping.GroupLabelKey == "" {
		return nil, errors.New("rankPodMapping.groupLabelKey is required")
	}
	if mapping.RankLabelKey == "" {
		return nil, errors.New("rankPodMapping.rankLabelKey is required")
	}
	if mapping.RanksPerPod < 0 {
		return nil, errors.New("rankPodMapping.ranksPerPod must be non-negative")
	}
	if config.SocketPort <= 0 {
		return nil, errors.New("podDiscoveryConfig.socketPort must be positive")
	}
	selector, err := labels.Parse(config.PodLabelSelector)
	if err != nil {
		return nil, fmt.Errorf("parse podDiscoveryConfig.podLabelSelector: %w", err)
	}
	ranksPerPod := mapping.RanksPerPod
	if ranksPerPod == 0 {
		ranksPerPod = 1
	}
	return &rankPodResolver{
		selector:        selector,
		namespace:       config.PodNamespace,
		groupLabelKey:   mapping.GroupLabelKey,
		rankLabelKey:    mapping.RankLabelKey,
		ranksPerPod:     ranksPerPod,
		socketPort:      config.SocketPort,
		replayPort:      config.EffectiveReplayPort(),
		endpoints:       make(map[types.NamespacedName]rankEndpointState),
		endpointsByRank: make(map[rankPodKey]map[types.NamespacedName]struct{}),
		workers:         make(map[rankPodKey]rankWorkerState),
		workerRanks:     make(map[types.NamespacedName][]rankPodKey),
		applied:         make(map[types.NamespacedName]rankSubscription),
		versions:        make(map[types.NamespacedName]uint64),
	}, nil
}

func (r *rankPodResolver) upsertEndpoint(meta *fwkdl.EndpointMetadata) []types.NamespacedName {
	if meta == nil || meta.ID.Name == "" {
		return nil
	}
	if meta.DataParallelRank == nil {
		return r.removeEndpoint(meta.ID)
	}
	group := meta.Labels[r.groupLabelKey]
	if group == "" || *meta.DataParallelRank < 0 {
		return r.removeEndpoint(meta.ID)
	}
	key := rankPodKey{namespace: meta.ID.Namespace, group: group, rank: *meta.DataParallelRank}

	r.mu.Lock()
	defer r.mu.Unlock()
	if previous, ok := r.endpoints[meta.ID]; ok && previous.key != key {
		r.removeEndpointFromRankLocked(meta.ID, previous.key)
	}
	r.endpoints[meta.ID] = rankEndpointState{key: key, metadata: meta.Clone()}
	if r.endpointsByRank[key] == nil {
		r.endpointsByRank[key] = make(map[types.NamespacedName]struct{})
	}
	r.endpointsByRank[key][meta.ID] = struct{}{}
	r.versions[meta.ID]++
	return []types.NamespacedName{meta.ID}
}

func (r *rankPodResolver) removeEndpoint(id types.NamespacedName) []types.NamespacedName {
	if id.Name == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if previous, ok := r.endpoints[id]; ok {
		delete(r.endpoints, id)
		r.removeEndpointFromRankLocked(id, previous.key)
	}
	r.versions[id]++
	return []types.NamespacedName{id}
}

func (r *rankPodResolver) removeEndpointFromRankLocked(id types.NamespacedName, key rankPodKey) {
	delete(r.endpointsByRank[key], id)
	if len(r.endpointsByRank[key]) == 0 {
		delete(r.endpointsByRank, key)
	}
}

func (r *rankPodResolver) upsertPod(pod *corev1.Pod) ([]types.NamespacedName, error) {
	if pod == nil {
		return nil, nil
	}
	podKey := types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}
	if (r.namespace != "" && pod.Namespace != r.namespace) ||
		!r.selector.Matches(labels.Set(pod.Labels)) ||
		!podutil.IsPodReady(pod) || pod.Status.PodIP == "" {
		return r.removePod(podKey), nil
	}
	group := pod.Labels[r.groupLabelKey]
	if group == "" {
		return r.removePod(podKey), fmt.Errorf("pod %s is missing rank group label %q", podKey, r.groupLabelKey)
	}
	workerIndex, err := strconv.Atoi(pod.Labels[r.rankLabelKey])
	if err != nil || workerIndex < 0 {
		return r.removePod(podKey), fmt.Errorf("pod %s has invalid rank label %q=%q", podKey, r.rankLabelKey, pod.Labels[r.rankLabelKey])
	}
	startRank := workerIndex * r.ranksPerPod
	ranks := make([]rankPodKey, 0, r.ranksPerPod)
	for offset := 0; offset < r.ranksPerPod; offset++ {
		ranks = append(ranks, rankPodKey{namespace: pod.Namespace, group: group, rank: startRank + offset})
	}

	r.mu.Lock()
	affected := make(map[types.NamespacedName]struct{})
	r.removePodLocked(podKey, affected)
	worker := rankWorkerState{pod: podKey, ip: pod.Status.PodIP}
	for _, rankKey := range ranks {
		r.workers[rankKey] = worker
		for endpointID := range r.endpointsByRank[rankKey] {
			affected[endpointID] = struct{}{}
			r.versions[endpointID]++
		}
	}
	r.workerRanks[podKey] = ranks
	r.mu.Unlock()
	return sortedEndpointIDs(affected), nil
}

func (r *rankPodResolver) removePod(podKey types.NamespacedName) []types.NamespacedName {
	r.mu.Lock()
	defer r.mu.Unlock()
	affected := make(map[types.NamespacedName]struct{})
	r.removePodLocked(podKey, affected)
	return sortedEndpointIDs(affected)
}

func (r *rankPodResolver) removePodLocked(podKey types.NamespacedName, affected map[types.NamespacedName]struct{}) {
	for _, rankKey := range r.workerRanks[podKey] {
		if worker, ok := r.workers[rankKey]; ok && worker.pod == podKey {
			delete(r.workers, rankKey)
			for endpointID := range r.endpointsByRank[rankKey] {
				affected[endpointID] = struct{}{}
				r.versions[endpointID]++
			}
		}
	}
	delete(r.workerRanks, podKey)
}

func sortedEndpointIDs(ids map[types.NamespacedName]struct{}) []types.NamespacedName {
	result := make([]types.NamespacedName, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result
}

func (r *rankPodResolver) plan(id types.NamespacedName) (*rankSubscription, *rankSubscription, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var previous *rankSubscription
	if current, ok := r.applied[id]; ok {
		copy := current
		previous = &copy
	}
	desired, err := r.desiredLocked(id)
	return previous, desired, r.versions[id], err
}

func (r *rankPodResolver) desiredLocked(id types.NamespacedName) (*rankSubscription, error) {
	endpoint, ok := r.endpoints[id]
	if !ok || endpoint.metadata.Address == "" || endpoint.metadata.Port == "" {
		return nil, nil
	}
	worker, ok := r.workers[endpoint.key]
	if !ok {
		return nil, nil
	}
	localRank := endpoint.key.rank % r.ranksPerPod
	transportPort := r.socketPort + localRank
	if transportPort > 65535 {
		return nil, fmt.Errorf("KV-event port for local rank %d exceeds 65535", localRank)
	}
	replayEndpoint := ""
	if r.replayPort > 0 {
		replayPort := r.replayPort + localRank
		if replayPort > 65535 {
			return nil, fmt.Errorf("KV-event replay port for local rank %d exceeds 65535", localRank)
		}
		replayEndpoint = "tcp://" + net.JoinHostPort(worker.ip, strconv.Itoa(replayPort))
	}
	return &rankSubscription{
		id:                id,
		sourceEndpoint:    fmt.Sprintf("%s:%s", endpoint.metadata.Address, endpoint.metadata.Port),
		transportEndpoint: "tcp://" + net.JoinHostPort(worker.ip, strconv.Itoa(transportPort)),
		replayEndpoint:    replayEndpoint,
		rank:              endpoint.key.rank,
	}, nil
}

func (r *rankPodResolver) recordApplied(id types.NamespacedName, applied *rankSubscription, version uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if applied == nil {
		delete(r.applied, id)
	} else {
		r.applied[id] = *applied
	}
	if r.versions[id] != version {
		return false
	}
	desired, err := r.desiredLocked(id)
	return err == nil && desired.equal(applied)
}

type rankEndpointHandler struct {
	producer *Producer
}

func (h *rankEndpointHandler) TypedName() fwkplugin.TypedName {
	tn := h.producer.typedName
	tn.Name += "/rank-endpoint"
	return tn
}

func (h *rankEndpointHandler) Extract(ctx context.Context, event fwkdl.EndpointEvent) error {
	if h == nil || h.producer == nil || h.producer.rankPodResolver == nil || event.Endpoint == nil {
		return nil
	}
	meta := event.Endpoint.GetMetadata()
	if meta == nil || meta.ID.Name == "" {
		return nil
	}
	var affected []types.NamespacedName
	if event.Type == fwkdl.EventDelete {
		affected = h.producer.rankPodResolver.removeEndpoint(meta.ID)
	} else {
		affected = h.producer.rankPodResolver.upsertEndpoint(meta)
	}
	return h.producer.reconcileRankEndpoints(ctx, affected)
}

type rankPodNotificationHandler struct {
	producer *Producer
}

func (h *rankPodNotificationHandler) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: rankPodExtractorType, Name: h.producer.typedName.Name + "/rank-pod"}
}

func (h *rankPodNotificationHandler) GVK() schema.GroupVersionKind { return podGVK }

func (h *rankPodNotificationHandler) Extract(ctx context.Context, event fwkdl.NotificationEvent) error {
	if h == nil || h.producer == nil || h.producer.rankPodResolver == nil || event.Object == nil {
		return nil
	}
	podKey := types.NamespacedName{Name: event.Object.GetName(), Namespace: event.Object.GetNamespace()}
	if event.Type == fwkdl.EventDelete {
		return h.producer.reconcileRankEndpoints(ctx, h.producer.rankPodResolver.removePod(podKey))
	}
	pod := &corev1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(event.Object.Object, pod); err != nil {
		return fmt.Errorf("convert Pod notification %s: %w", podKey, err)
	}
	affected, podErr := h.producer.rankPodResolver.upsertPod(pod)
	return errors.Join(podErr, h.producer.reconcileRankEndpoints(ctx, affected))
}

func (p *Producer) reconcileRankEndpoints(ctx context.Context, endpointIDs []types.NamespacedName) error {
	var errs []error
	for _, endpointID := range endpointIDs {
		if err := p.reconcileRankEndpoint(ctx, endpointID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *Producer) reconcileRankEndpoint(ctx context.Context, endpointID types.NamespacedName) error {
	const maxAttempts = 8
	for range maxAttempts {
		previous, desired, version, err := p.rankPodResolver.plan(endpointID)
		if err != nil {
			return err
		}
		if previous.equal(desired) {
			return nil
		}

		if previous != nil {
			p.subscribersManager.RemoveSubscriber(ctx, endpointID.String())
			p.clearRankSubscription(ctx, previous)
		}

		var applied *rankSubscription
		if desired != nil {
			rank := desired.rank
			if err := p.subscribersManager.EnsureSubscriber(
				p.subscriberCtx,
				endpointID.String(),
				desired.sourceEndpoint,
				desired.transportEndpoint,
				desired.replayEndpoint,
				p.kvEventsConfig.TopicFilter,
				&rank,
				true,
			); err != nil {
				p.rankPodResolver.recordApplied(endpointID, nil, version)
				return fmt.Errorf("ensure subscriber for %s: %w", endpointID, err)
			}
			applied = desired
		}
		if p.rankPodResolver.recordApplied(endpointID, applied, version) {
			return nil
		}
	}
	return fmt.Errorf("reconcile rank-pod subscriber for %s did not converge", endpointID)
}

func (p *Producer) clearRankSubscription(ctx context.Context, subscription *rankSubscription) {
	if subscription == nil || p.kvCacheIndexer == nil || p.kvCacheIndexer.KVBlockIndex() == nil {
		return
	}
	if err := kvblock.ClearDataParallelRank(ctx, p.kvCacheIndexer.KVBlockIndex(),
		subscription.sourceEndpoint, subscription.rank); err != nil {
		log.FromContext(ctx).WithName(p.typedName.String()).Error(err,
			"Failed to clear index entries for rank-pod subscriber",
			"endpoint", subscription.id.String(),
			"sourceEndpoint", subscription.sourceEndpoint,
			"dataParallelRank", subscription.rank)
	}
}

func (p *Producer) registerRankPodDependency(registrar fwkdl.Registrar) error {
	if p.rankPodResolver == nil {
		return nil
	}
	return registrar.Register(fwkdl.PendingRegistration{
		Owner:      p.typedName,
		SourceType: sourcenotifications.NotificationSourceType,
		Extractor:  &rankPodNotificationHandler{producer: p},
		DefaultSource: sourcenotifications.NewK8sNotificationSource(
			sourcenotifications.NotificationSourceType,
			p.typedName.Name+"/rank-pod",
			podGVK,
		),
	})
}
