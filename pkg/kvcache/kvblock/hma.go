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

import "sync"

// GroupID identifies a vLLM KV cache group.
type GroupID int

// GroupMetadata holds per-group KV cache spec info learned from BlockStored events.
type GroupMetadata struct {
	Kind              string
	BlockSize         int
	SlidingWindowSize *int
}

// GroupCatalog is a thread-safe catalog of per-pod KV cache group metadata.
type GroupCatalog struct {
	mu      sync.RWMutex
	entries map[string]map[GroupID]GroupMetadata
}

// GroupCatalogSnapshotEntry holds one pod's KV cache group metadata.
type GroupCatalogSnapshotEntry struct {
	PodIdentifier string        `json:"podIdentifier"`
	GroupID       GroupID       `json:"groupId"`
	Metadata      GroupMetadata `json:"metadata"`
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

// Snapshot returns a copy of all learned group metadata.
func (c *GroupCatalog) Snapshot() []GroupCatalogSnapshotEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]GroupCatalogSnapshotEntry, 0)
	for podIdentifier, groups := range c.entries {
		for groupID, metadata := range groups {
			result = append(result, GroupCatalogSnapshotEntry{
				PodIdentifier: podIdentifier, GroupID: groupID, Metadata: metadata,
			})
		}
	}
	return result
}

// Restore replaces the catalog with snapshot entries.
func (c *GroupCatalog) Restore(snapshot []GroupCatalogSnapshotEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]map[GroupID]GroupMetadata)
	for _, entry := range snapshot {
		groups := c.entries[entry.PodIdentifier]
		if groups == nil {
			groups = make(map[GroupID]GroupMetadata)
			c.entries[entry.PodIdentifier] = groups
		}
		groups[entry.GroupID] = entry.Metadata
	}
}

// Clear removes all group metadata for a pod.
func (c *GroupCatalog) Clear(podIdentifier string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, podIdentifier)
}
