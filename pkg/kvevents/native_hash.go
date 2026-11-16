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

package kvevents

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func (p *Pool) processNativeEvent(ctx context.Context, event GenericEvent, pod, model string) {
	var hashes []uint64
	var group *int
	var tier string
	store := false
	switch ev := event.(type) {
	case *BlockStoredEvent:
		hashes, group, tier = ev.BlockHashes, ev.GroupIdx, ev.DeviceTier
		store = true
	case *BlockRemovedEvent:
		hashes, group, tier = ev.BlockHashes, ev.GroupIdx, ev.DeviceTier
	case *AllBlocksClearedEvent:
		p.clearPod(ctx, pod, false)
		return
	default:
		return
	}
	if group != nil && *group < 0 {
		return
	}
	tier = normalizeDeviceTier(tier)
	scope := blockScope{
		podIdentifier: pod, deviceTier: tier,
		groupIdx: groupIdxOrNoGroup(group), dataParallelRank: noDataParallelRank,
	}
	// Reference counts must use the same model namespace as index entries.
	scopedHashes := make([]uint64, len(hashes))
	for i, hash := range hashes {
		scopedHashes[i] = uint64(kvblock.NativeBlockKey(model, group, hash))
	}
	hashes = scopedHashes
	if !store {
		hashes = p.dedup.filterRemove(scope, hashes)
	}
	if len(hashes) == 0 {
		return
	}
	keys := make([]kvblock.BlockHash, len(hashes))
	for i, hash := range hashes {
		keys[i] = kvblock.BlockHash(hash)
	}
	entry := kvblock.PodEntry{PodIdentifier: pod, DeviceTier: tier}
	if group != nil {
		entry.HasGroup, entry.GroupIdx = true, kvblock.GroupID(*group)
	}
	var err error
	if store {
		err = p.index.Add(ctx, nil, keys, []kvblock.PodEntry{entry})
		if err == nil {
			p.dedup.trackStore(scope, hashes)
		}
	} else {
		err = p.index.Evict(ctx, kvblock.RequestKey, keys, []kvblock.PodEntry{entry})
	}
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to update native hash index", "podIdentifier", pod)
	}
}
