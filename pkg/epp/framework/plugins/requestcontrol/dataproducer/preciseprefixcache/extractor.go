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

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

var _ fwkdl.EndpointExtractor = &Producer{}

// Extract processes endpoint lifecycle events emitted by the
// endpoint-notification-source: add/update installs a per-pod ZMQ KV-events
// subscriber, delete tears one down. No-op unless per-pod discovery is
// enabled.
func (p *Producer) Extract(ctx context.Context, event fwkdl.EndpointEvent) error {
	if !p.kvEventsConfig.DiscoverPods || p.kvEventsConfig.PodDiscoveryConfig == nil {
		return nil
	}
	meta := event.Endpoint.GetMetadata()
	if meta == nil || meta.ID.Name == "" {
		return nil
	}

	logger := log.FromContext(ctx).WithName(p.typedName.String())
	endpointKey := meta.ID.String()

	switch event.Type {
	case fwkdl.EventAddOrUpdate:
		if err := p.ensureSubscriber(ctx, meta); err != nil {
			return err
		}
		logger.V(logging.DEBUG).Info("Adding subscriber", "endpoint", endpointKey)
	case fwkdl.EventDelete:
		for _, subscriberID := range p.subscriberIDs(meta) {
			p.subscribersManager.RemoveSubscriber(ctx, subscriberID)
		}
		if meta.Address != "" {
			if err := p.kvCacheIndexer.KVBlockIndex().Clear(ctx, fmt.Sprintf("%s:%s", meta.Address, meta.Port)); err != nil {
				logger.Error(err, "Failed to clear index entries for removed endpoint",
					"endpoint", endpointKey, "address", meta.Address, "port", meta.Port)
			}
		}
		logger.V(logging.DEBUG).Info("Removed KV-events subscriber", "endpoint", endpointKey)
	}
	return nil
}

// ensureSubscriber idempotently installs the KV-events subscribers for an
// endpoint. Shared-port DP endpoints get one subscriber per configured rank;
// rank-specific endpoints use their metadata rank.
func (p *Producer) ensureSubscriber(ctx context.Context, meta *fwkdl.EndpointMetadata) error {
	if meta == nil || meta.Address == "" {
		return nil
	}
	sourceEndpoint := fmt.Sprintf("%s:%s", meta.Address, meta.Port)
	logger := log.FromContext(ctx).WithName(p.typedName.String())
	ranks := p.subscriberRanks(meta)
	ids := p.subscriberIDs(meta)
	for i, rank := range ranks {
		port := p.kvEventsConfig.PodDiscoveryConfig.SocketPort + rank
		zmqEndpoint := fmt.Sprintf("tcp://%s:%d", meta.Address, port)
		replayEndpoint := ""
		if replayPort := p.kvEventsConfig.PodDiscoveryConfig.EffectiveReplayPort(); replayPort > 0 {
			replayEndpoint = fmt.Sprintf("tcp://%s:%d", meta.Address, replayPort+rank)
		}
		var dataParallelRank *int
		if len(ranks) > 1 {
			dataParallelRank = &rank
		}
		// subscriberCtx is plugin-lifetime; caller ctx would tear subscribers
		// down on request completion.
		if err := p.subscribersManager.EnsureSubscriber(p.subscriberCtx, ids[i],
			sourceEndpoint, zmqEndpoint, replayEndpoint, p.kvEventsConfig.TopicFilter,
			dataParallelRank, true); err != nil {
			logger.Error(err, "Failed to ensure KV-events subscriber for endpoint",
				"endpoint", ids[i], "address", meta.Address)
			return fmt.Errorf("ensure subscriber for %s: %w", ids[i], err)
		}
		logger.V(logging.DEBUG).Info("Ensured KV-events subscriber",
			"endpoint", ids[i], "zmq", zmqEndpoint, "replay", replayEndpoint)
	}
	return nil
}

func (p *Producer) subscriberRanks(meta *fwkdl.EndpointMetadata) []int {
	size := p.kvEventsConfig.PodDiscoveryConfig.DataParallelSize
	if size <= 1 {
		return []int{meta.GetRankIndex()}
	}
	ranks := make([]int, size)
	for rank := range size {
		ranks[rank] = rank
	}
	return ranks
}

func (p *Producer) subscriberIDs(meta *fwkdl.EndpointMetadata) []string {
	endpointKey := meta.ID.String()
	ranks := p.subscriberRanks(meta)
	if len(ranks) == 1 {
		return []string{endpointKey}
	}
	ids := make([]string, len(ranks))
	for i, rank := range ranks {
		ids[i] = fmt.Sprintf("%s@dp%d", endpointKey, rank)
	}
	return ids
}
