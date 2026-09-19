// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvevents

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

const checkpointSchemaVersion = 1

// ErrCheckpointReplayInProgress reports that a stream is rebuilding state and
// a durable snapshot would not represent a replay boundary.
var ErrCheckpointReplayInProgress = errors.New("cannot checkpoint while KV-event replay is in progress")

type checkpointPayload struct {
	SchemaVersion            int                                 `json:"schemaVersion"`
	CreatedAt                time.Time                           `json:"createdAt"`
	ConfigurationFingerprint string                              `json:"configurationFingerprint"`
	Index                    kvblock.IndexSnapshot               `json:"index"`
	Dedup                    []dedupSnapshotEntry                `json:"dedup"`
	Groups                   []kvblock.GroupCatalogSnapshotEntry `json:"groups"`
	LastConsumedSequences    map[string]uint64                   `json:"lastConsumedSequences"`
}

type checkpointEnvelope struct {
	Checksum string          `json:"checksum"`
	Payload  json.RawMessage `json:"payload"`
}

// CheckpointResult describes a completed checkpoint capture.
type CheckpointResult struct {
	CreatedAt time.Time
	Sources   int
	Bytes     int
}

// MarshalCheckpoint returns a consistent snapshot of the event pool.
func (p *Pool) MarshalCheckpoint(configurationFingerprint string) ([]byte, CheckpointResult, error) {
	if !p.checkpointEnabled.Load() {
		return nil, CheckpointResult{}, errors.New("checkpointing is not enabled")
	}
	snapshotter, ok := p.index.(kvblock.Snapshotter)
	if !ok {
		return nil, CheckpointResult{}, errors.New("index backend does not support checkpoints")
	}
	p.checkpointWriteMu.Lock()
	defer p.checkpointWriteMu.Unlock()

	p.checkpointMu.Lock()
	p.lastConsumedMu.RLock()
	if len(p.replaysInProgress) > 0 {
		p.lastConsumedMu.RUnlock()
		p.checkpointMu.Unlock()
		return nil, CheckpointResult{}, ErrCheckpointReplayInProgress
	}
	lastConsumedSequences := make(map[string]uint64, len(p.lastConsumedSeq))
	for source, sequence := range p.lastConsumedSeq {
		lastConsumedSequences[source] = sequence
	}
	p.lastConsumedMu.RUnlock()
	indexSnapshot, err := snapshotter.Snapshot()
	if err != nil {
		p.checkpointMu.Unlock()
		return nil, CheckpointResult{}, fmt.Errorf("snapshot index: %w", err)
	}
	dedupSnapshot := p.dedup.snapshot()
	groupSnapshot := p.groupCatalog.Snapshot()
	p.checkpointMu.Unlock()

	createdAt := time.Now().UTC()
	payload := checkpointPayload{
		SchemaVersion:            checkpointSchemaVersion,
		CreatedAt:                createdAt,
		ConfigurationFingerprint: configurationFingerprint,
		Index:                    indexSnapshot,
		Dedup:                    dedupSnapshot,
		Groups:                   groupSnapshot,
		LastConsumedSequences:    lastConsumedSequences,
	}
	encoded, err := marshalCheckpoint(payload)
	if err != nil {
		return nil, CheckpointResult{}, fmt.Errorf("encode checkpoint: %w", err)
	}
	return encoded, CheckpointResult{
		CreatedAt: createdAt,
		Sources:   len(lastConsumedSequences),
		Bytes:     len(encoded),
	}, nil
}

// RestoreCheckpoint restores a checkpoint before event workers start.
func (p *Pool) RestoreCheckpoint(data []byte, configurationFingerprint string) error {
	if p.started.Load() {
		return errors.New("checkpoint must be restored before the event pool starts")
	}
	if err := p.EnableCheckpointing(); err != nil {
		return err
	}
	payload, err := unmarshalCheckpoint(data)
	if err != nil {
		return err
	}
	if payload.SchemaVersion != checkpointSchemaVersion {
		return fmt.Errorf("unsupported checkpoint schema version %d", payload.SchemaVersion)
	}
	if payload.ConfigurationFingerprint != configurationFingerprint {
		return errors.New("checkpoint configuration does not match")
	}
	snapshotter, ok := p.index.(kvblock.Snapshotter)
	if !ok {
		return errors.New("index backend does not support checkpoints")
	}
	if err := validateCheckpointPayload(payload, snapshotter); err != nil {
		return err
	}
	if err := snapshotter.Restore(payload.Index); err != nil {
		return fmt.Errorf("restore index: %w", err)
	}
	p.dedup.restore(payload.Dedup)
	p.groupCatalog.Restore(payload.Groups)
	p.lastConsumedMu.Lock()
	p.lastConsumedSeq = make(map[string]uint64, len(payload.LastConsumedSequences))
	p.replaysInProgress = make(map[string]struct{})
	for source, sequence := range payload.LastConsumedSequences {
		p.lastConsumedSeq[source] = sequence
	}
	p.lastConsumedMu.Unlock()
	return nil
}

func validateCheckpointPayload(payload checkpointPayload, snapshotter kvblock.Snapshotter) error {
	if err := snapshotter.ValidateSnapshot(payload.Index); err != nil {
		return fmt.Errorf("validate index snapshot: %w", err)
	}
	type dedupIdentity struct {
		podIdentifier    string
		deviceTier       string
		groupIdx         int
		dataParallelRank int
		blockHash        uint64
	}
	dedupEntries := make(map[dedupIdentity]struct{}, len(payload.Dedup))
	for _, entry := range payload.Dedup {
		if entry.PodIdentifier == "" || entry.Count <= 0 {
			return errors.New("checkpoint contains invalid dedup state")
		}
		identity := dedupIdentity{
			podIdentifier:    entry.PodIdentifier,
			deviceTier:       entry.DeviceTier,
			groupIdx:         entry.GroupIdx,
			dataParallelRank: entry.DataParallelRank,
			blockHash:        entry.BlockHash,
		}
		if _, exists := dedupEntries[identity]; exists {
			return errors.New("checkpoint contains duplicate dedup state")
		}
		dedupEntries[identity] = struct{}{}
	}
	groups := make(map[string]map[kvblock.GroupID]struct{})
	for _, entry := range payload.Groups {
		if entry.PodIdentifier == "" || entry.GroupID < 0 || entry.Metadata.BlockSize <= 0 ||
			(entry.Metadata.SlidingWindowSize != nil && *entry.Metadata.SlidingWindowSize <= 0) {
			return errors.New("checkpoint contains invalid group state")
		}
		if groups[entry.PodIdentifier] == nil {
			groups[entry.PodIdentifier] = make(map[kvblock.GroupID]struct{})
		}
		if _, exists := groups[entry.PodIdentifier][entry.GroupID]; exists {
			return errors.New("checkpoint contains duplicate group state")
		}
		groups[entry.PodIdentifier][entry.GroupID] = struct{}{}
	}
	for source := range payload.LastConsumedSequences {
		if source == "" {
			return errors.New("checkpoint contains an empty event source")
		}
	}
	return nil
}
