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
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvevents"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultSpeculativeTTL      = 2 * time.Second
	experimentalPrefillProfile = "prefill"
	blockKeysStateKey          = plugin.StateKey("precise-prefix-cache-producer.block-keys")
)

var _ requestcontrol.PreRequest = &Producer{}

// speculativeEntries records the speculative rows added on a routing decision
// so the TTL-eviction callback can roll them back.
type speculativeEntries struct {
	perPromptKeys [][]kvblock.BlockHash
	podEntries    []kvblock.PodEntry
}

// blockKeysState carries the block keys computed in Produce to PreRequest
// via PluginState, avoiding a second hash on the same request.
// perPromptKeys holds one slice of block keys per prompt; single-prompt
// requests use a length-1 outer slice.
type blockKeysState struct {
	perPromptKeys [][]kvblock.BlockHash
}

// Clone implements plugin.StateData.
func (s *blockKeysState) Clone() plugin.StateData {
	cp := make([][]kvblock.BlockHash, len(s.perPromptKeys))
	for i, keys := range s.perPromptKeys {
		cp[i] = make([]kvblock.BlockHash, len(keys))
		copy(cp[i], keys)
	}
	return &blockKeysState{perPromptKeys: cp}
}

// buildSpeculativeCache constructs the TTL cache used to evict speculative
// index entries. Returns (nil, 0, nil) when speculative indexing is disabled.
// The cache and its background goroutine are bound to ctx. Expired entries
// are evicted through the event pool's writer so the index keeps a single
// mutator.
func buildSpeculativeCache(ctx context.Context, config PluginConfig,
	pool *kvevents.Pool,
) (*ttlcache.Cache[string, *speculativeEntries], time.Duration, error) {
	if !config.SpeculativeIndexing {
		return nil, 0, nil
	}

	ttl := defaultSpeculativeTTL
	if config.SpeculativeTTL != "" {
		parsed, err := time.ParseDuration(config.SpeculativeTTL)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid speculativeTTL %q: %w", config.SpeculativeTTL, err)
		}
		if parsed > 0 {
			ttl = parsed
		}
	}

	cache := ttlcache.New[string, *speculativeEntries](
		ttlcache.WithTTL[string, *speculativeEntries](ttl),
	)
	cache.OnEviction(func(_ context.Context, reason ttlcache.EvictionReason,
		item *ttlcache.Item[string, *speculativeEntries],
	) {
		if reason != ttlcache.EvictionReasonExpired {
			return
		}
		entries := item.Value()
		pool.Submit(func(ctx context.Context, index kvblock.Index) {
			for _, promptKeys := range entries.perPromptKeys {
				for _, reqKey := range promptKeys {
					//nolint:errcheck // best-effort cleanup on TTL expiry
					index.Evict(ctx, reqKey, kvblock.RequestKey, entries.podEntries)
				}
			}
		})
	})
	go cache.Start()
	go func() {
		<-ctx.Done()
		cache.Stop()
	}()

	return cache, ttl, nil
}

// PreRequest seeds speculative KV-block index entries for the endpoint(s)
// selected by the scheduler, so the next same-prefix request hits without
// waiting for confirmed KV-events from the engine. Entries are tracked in
// a TTL cache and evicted automatically. No-op when speculativeIndexing
// is disabled.
func (p *Producer) PreRequest(ctx context.Context,
	request *scheduling.InferenceRequest, schedulingResult *scheduling.SchedulingResult,
) error {
	if !p.speculativeEnabled {
		return nil
	}

	logger := log.FromContext(ctx).WithName(p.typedName.String())

	state, err := plugin.ReadPluginStateKey[*blockKeysState](
		p.pluginState, request.RequestID, blockKeysStateKey)
	if err != nil {
		logger.V(logging.TRACE).Info("No plugin state for PreRequest, skipping speculative indexing",
			"requestID", request.RequestID)
		return nil
	}
	p.pluginState.Delete(request.RequestID)

	hasKeys := false
	for _, pk := range state.perPromptKeys {
		if len(pk) > 0 {
			hasKeys = true
			break
		}
	}
	if !hasKeys {
		return nil
	}

	primary := schedulingResult.ProfileResults[schedulingResult.PrimaryProfileName]
	if primary == nil || len(primary.TargetEndpoints) == 0 {
		return nil
	}
	targetEndpoint := primary.TargetEndpoints[0]
	targetMeta := targetEndpoint.GetMetadata()
	if targetMeta == nil {
		return nil
	}
	speculativePod := speculativePodEntry(targetMeta)

	promptKeys := state.perPromptKeys
	prefillPod, hasPrefill := prefillPod(schedulingResult)
	pods := []kvblock.PodEntry{speculativePod}
	if hasPrefill {
		pods = append(pods, prefillPod)
	}
	addSpeculative := func(ctx context.Context, index kvblock.Index) {
		logger := log.FromContext(ctx).WithName(p.typedName.String())
		// Insert per-prompt keys separately to preserve correct block adjacency.
		for _, pod := range pods {
			for _, keys := range promptKeys {
				if err := index.Add(ctx, nil, keys, []kvblock.PodEntry{pod}); err != nil {
					logger.Error(err, "Failed to add speculative entries to index",
						"pod", pod.PodIdentifier)
				}
			}
		}
	}
	if p.kvEventsPool != nil {
		if !p.kvEventsPool.TrySubmit(addSpeculative) {
			logger.Info("Dropped speculative index update while the event pool is saturated",
				"pod", speculativePod.PodIdentifier)
		}
	} else {
		addSpeculative(ctx, p.kvCacheIndexer.KVBlockIndex())
	}

	p.speculativeCache.Set(request.RequestID, &speculativeEntries{
		perPromptKeys: state.perPromptKeys,
		podEntries:    pods,
	}, p.speculativeTTL)

	logger.V(logging.TRACE).Info("Added speculative entries",
		"requestID", request.RequestID,
		"pod", speculativePod.PodIdentifier,
		"prompts", len(state.perPromptKeys),
		"ttl", p.speculativeTTL)
	return nil
}

// prefillPod returns the prefill endpoint's speculative entry for
// prefill-disaggregated scheduling, when the result selected one.
func prefillPod(schedulingResult *scheduling.SchedulingResult) (kvblock.PodEntry, bool) {
	pr, exists := schedulingResult.ProfileResults[experimentalPrefillProfile]
	if !exists || len(pr.TargetEndpoints) == 0 {
		return kvblock.PodEntry{}, false
	}
	meta := pr.TargetEndpoints[0].GetMetadata()
	if meta == nil {
		return kvblock.PodEntry{}, false
	}
	return speculativePodEntry(meta), true
}

func speculativePodEntry(meta *datalayer.EndpointMetadata) kvblock.PodEntry {
	entry := kvblock.PodEntry{
		PodIdentifier: fmt.Sprintf("%s:%s", meta.Address, meta.Port),
		Speculative:   true,
	}
	if meta.DataParallelRank != nil {
		rank := *meta.DataParallelRank
		entry.DataParallelRank = &rank
	}
	return entry
}
