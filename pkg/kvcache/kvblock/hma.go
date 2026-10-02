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

package kvblock

import (
	"sort"
	"sync"
)

// GroupID identifies a vLLM KV cache group.
type GroupID int

// GroupMetadata holds per-group KV cache spec info learned from BlockStored events.
type GroupMetadata struct {
	Kind              string
	BlockSize         int
	SlidingWindowSize *int
}

// GroupCatalogSnapshotEntry stores one learned cache-group definition.
type GroupCatalogSnapshotEntry struct {
	PodIdentifier string
	GroupID       GroupID
	Metadata      GroupMetadata
}

// GroupCatalog is a thread-safe catalog of per-pod KV cache group metadata.
type GroupCatalog struct {
	mu      sync.RWMutex
	entries map[string]map[GroupID]GroupMetadata
}

// NewGroupCatalog creates a new, empty GroupCatalog.
func NewGroupCatalog() *GroupCatalog {
	return &GroupCatalog{
		entries: make(map[string]map[GroupID]GroupMetadata),
	}
}

// Learn records group metadata for a pod.
func (c *GroupCatalog) Learn(podID string, g GroupID, meta GroupMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries[podID] == nil {
		c.entries[podID] = make(map[GroupID]GroupMetadata)
	}
	c.entries[podID][g] = meta
}

// Get returns the metadata for a pod group.
func (c *GroupCatalog) Get(podID string, g GroupID) (GroupMetadata, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	groups, ok := c.entries[podID]
	if !ok {
		return GroupMetadata{}, false
	}
	meta, ok := groups[g]
	return meta, ok
}

// Clear removes all group metadata for a pod.
func (c *GroupCatalog) Clear(podID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, podID)
}

// Snapshot returns a stable, sorted copy of the catalog.
func (c *GroupCatalog) Snapshot() []GroupCatalogSnapshotEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entries := make([]GroupCatalogSnapshotEntry, 0)
	for podID, groups := range c.entries {
		for groupID, metadata := range groups {
			if metadata.SlidingWindowSize != nil {
				value := *metadata.SlidingWindowSize
				metadata.SlidingWindowSize = &value
			}
			entries = append(entries, GroupCatalogSnapshotEntry{
				PodIdentifier: podID, GroupID: groupID, Metadata: metadata,
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].PodIdentifier != entries[j].PodIdentifier {
			return entries[i].PodIdentifier < entries[j].PodIdentifier
		}
		return entries[i].GroupID < entries[j].GroupID
	})
	return entries
}

// Restore replaces the catalog with checkpointed metadata.
func (c *GroupCatalog) Restore(entries []GroupCatalogSnapshotEntry) {
	restored := make(map[string]map[GroupID]GroupMetadata)
	for _, entry := range entries {
		if restored[entry.PodIdentifier] == nil {
			restored[entry.PodIdentifier] = make(map[GroupID]GroupMetadata)
		}
		metadata := entry.Metadata
		if metadata.SlidingWindowSize != nil {
			value := *metadata.SlidingWindowSize
			metadata.SlidingWindowSize = &value
		}
		restored[entry.PodIdentifier][entry.GroupID] = metadata
	}
	c.mu.Lock()
	c.entries = restored
	c.mu.Unlock()
}
