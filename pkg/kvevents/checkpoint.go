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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

const checkpointSchemaVersion = 1

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

// CheckpointResult describes a completed checkpoint write.
type CheckpointResult struct {
	Path      string    `json:"path"`
	CreatedAt time.Time `json:"createdAt"`
	Sources   int       `json:"sources"`
}

// WriteCheckpoint atomically replaces path with a consistent pool snapshot.
func (p *Pool) WriteCheckpoint(path, configurationFingerprint string) (CheckpointResult, error) {
	if path == "" {
		return CheckpointResult{}, errors.New("checkpoint path is not configured")
	}
	snapshotter, ok := p.index.(kvblock.Snapshotter)
	if !ok {
		return CheckpointResult{}, errors.New("index backend does not support checkpoints")
	}
	p.checkpointWriteMu.Lock()
	defer p.checkpointWriteMu.Unlock()

	p.checkpointMu.Lock()
	indexSnapshot, err := snapshotter.Snapshot()
	if err != nil {
		p.checkpointMu.Unlock()
		return CheckpointResult{}, fmt.Errorf("snapshot index: %w", err)
	}
	p.lastConsumedMu.RLock()
	if len(p.replaysInProgress) > 0 {
		p.lastConsumedMu.RUnlock()
		p.checkpointMu.Unlock()
		return CheckpointResult{}, errors.New("cannot checkpoint while KV-event replay is in progress")
	}
	lastConsumedSequences := make(map[string]uint64, len(p.lastConsumedSeq))
	for source, seq := range p.lastConsumedSeq {
		lastConsumedSequences[source] = seq
	}
	p.lastConsumedMu.RUnlock()
	dedupSnapshot := p.dedup.snapshot()
	groupSnapshot := p.groupCatalog.Snapshot()
	p.checkpointMu.Unlock()

	createdAt := time.Now().UTC()
	payload, err := json.Marshal(checkpointPayload{
		SchemaVersion: checkpointSchemaVersion, CreatedAt: createdAt,
		ConfigurationFingerprint: configurationFingerprint, Index: indexSnapshot,
		Dedup: dedupSnapshot, Groups: groupSnapshot, LastConsumedSequences: lastConsumedSequences,
	})
	if err != nil {
		return CheckpointResult{}, fmt.Errorf("encode checkpoint payload: %w", err)
	}
	digest := sha256.Sum256(payload)
	encoded, err := json.Marshal(checkpointEnvelope{
		Checksum: hex.EncodeToString(digest[:]), Payload: payload,
	})
	if err != nil {
		return CheckpointResult{}, fmt.Errorf("encode checkpoint: %w", err)
	}
	if err := writeFileAtomic(path, encoded, 0o600); err != nil {
		return CheckpointResult{}, err
	}
	return CheckpointResult{Path: path, CreatedAt: createdAt, Sources: len(lastConsumedSequences)}, nil
}

// RestoreCheckpoint restores a checkpoint if path exists. It must be called
// before event workers and subscribers start.
func (p *Pool) RestoreCheckpoint(path, configurationFingerprint string) (bool, error) {
	if path == "" {
		return false, nil
	}
	if p.started.Load() {
		return false, errors.New("checkpoint must be restored before the event pool starts")
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read checkpoint: %w", err)
	}
	var envelope checkpointEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("decode checkpoint: %w", err)
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.Checksum != hex.EncodeToString(digest[:]) {
		return false, errors.New("checkpoint checksum mismatch")
	}
	var payload checkpointPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode checkpoint payload: %w", err)
	}
	if payload.SchemaVersion != checkpointSchemaVersion {
		return false, fmt.Errorf("unsupported checkpoint schema version %d", payload.SchemaVersion)
	}
	if payload.ConfigurationFingerprint != configurationFingerprint {
		return false, errors.New("checkpoint configuration does not match")
	}
	snapshotter, ok := p.index.(kvblock.Snapshotter)
	if !ok {
		return false, errors.New("index backend does not support checkpoints")
	}
	if err := validateCheckpointPayload(payload, snapshotter); err != nil {
		return false, err
	}
	if err := snapshotter.Restore(payload.Index); err != nil {
		return false, fmt.Errorf("restore index: %w", err)
	}
	p.dedup.restore(payload.Dedup)
	p.groupCatalog.Restore(payload.Groups)
	p.lastConsumedMu.Lock()
	p.lastConsumedSeq = make(map[string]uint64, len(payload.LastConsumedSequences))
	p.replaysInProgress = make(map[string]struct{})
	for source, seq := range payload.LastConsumedSequences {
		p.lastConsumedSeq[source] = seq
	}
	p.lastConsumedMu.Unlock()
	return true, nil
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
			podIdentifier: entry.PodIdentifier, deviceTier: entry.DeviceTier,
			groupIdx: entry.GroupIdx, dataParallelRank: entry.DataParallelRank,
			blockHash: entry.BlockHash,
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

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".checkpoint-*")
	if err != nil {
		return fmt.Errorf("create checkpoint temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return fmt.Errorf("set checkpoint permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace checkpoint: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open checkpoint directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint directory: %w", err)
	}
	return nil
}
