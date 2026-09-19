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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

const (
	checkpointBinaryMagic   = "LLMDCP\x00"
	checkpointBinaryVersion = byte(1)
	checkpointBinaryHeader  = len(checkpointBinaryMagic) + 1 + sha256.Size

	wirePodSpeculative = 1 << iota
	wirePodHasGroup
)

// The wire structs use MessagePack arrays so field names are not repeated for
// every index entry. Repeated strings are represented by indexes into Strings.
type checkpointWirePayload struct {
	_msgpack                 struct{} `msgpack:",as_array"`
	CreatedAtUnixNano        int64
	ConfigurationFingerprint string
	Strings                  []string
	Entries                  []checkpointWireEntry
	EngineMappings           []checkpointWireEngineMapping
	Dedup                    []checkpointWireDedupEntry
	Groups                   []checkpointWireGroupEntry
	LastConsumedSequences    []checkpointWireSequence
}

type checkpointWireEntry struct {
	_msgpack   struct{} `msgpack:",as_array"`
	RequestKey uint64
	Pods       []checkpointWirePod
}

type checkpointWirePod struct {
	_msgpack      struct{} `msgpack:",as_array"`
	PodIdentifier uint32
	DeviceTier    uint32
	Flags         uint8
	GroupID       int64
}

type checkpointWireEngineMapping struct {
	_msgpack    struct{} `msgpack:",as_array"`
	EngineKey   uint64
	RequestKeys []uint64
}

type checkpointWireDedupEntry struct {
	_msgpack         struct{} `msgpack:",as_array"`
	PodIdentifier    uint32
	DeviceTier       uint32
	GroupIdx         int64
	DataParallelRank int64
	BlockHash        uint64
	Count            uint64
}

type checkpointWireGroupEntry struct {
	_msgpack          struct{} `msgpack:",as_array"`
	PodIdentifier     uint32
	GroupID           int64
	Kind              uint32
	BlockSize         uint64
	SlidingWindowSize int64
}

type checkpointWireSequence struct {
	_msgpack struct{} `msgpack:",as_array"`
	Source   uint32
	Sequence uint64
}

type checkpointStringTable struct {
	values []string
	index  map[string]uint32
}

func marshalCheckpoint(payload checkpointPayload) ([]byte, error) {
	wire := encodeCheckpointPayload(payload)
	var payloadBuffer bytes.Buffer
	encoder := msgpack.NewEncoder(&payloadBuffer)
	encoder.UseCompactInts(true)
	if err := encoder.Encode(wire); err != nil {
		return nil, fmt.Errorf("encode binary checkpoint payload: %w", err)
	}

	payloadBytes := payloadBuffer.Bytes()
	digest := sha256.Sum256(payloadBytes)
	encoded := make([]byte, 0, checkpointBinaryHeader+len(payloadBytes))
	encoded = append(encoded, checkpointBinaryMagic...)
	encoded = append(encoded, checkpointBinaryVersion)
	encoded = append(encoded, digest[:]...)
	encoded = append(encoded, payloadBytes...)
	return encoded, nil
}

func unmarshalCheckpoint(data []byte) (checkpointPayload, error) {
	if hasBinaryCheckpointHeader(data) {
		return unmarshalBinaryCheckpoint(data)
	}
	return unmarshalLegacyCheckpoint(data)
}

func hasBinaryCheckpointHeader(data []byte) bool {
	return len(data) >= len(checkpointBinaryMagic) && string(data[:len(checkpointBinaryMagic)]) == checkpointBinaryMagic
}

func unmarshalBinaryCheckpoint(data []byte) (checkpointPayload, error) {
	if len(data) < checkpointBinaryHeader {
		return checkpointPayload{}, errors.New("binary checkpoint header is truncated")
	}
	version := data[len(checkpointBinaryMagic)]
	if version != checkpointBinaryVersion {
		return checkpointPayload{}, fmt.Errorf("unsupported binary checkpoint version %d", version)
	}
	wantChecksum := data[len(checkpointBinaryMagic)+1 : checkpointBinaryHeader]
	payloadBytes := data[checkpointBinaryHeader:]
	gotChecksum := sha256.Sum256(payloadBytes)
	if !bytes.Equal(wantChecksum, gotChecksum[:]) {
		return checkpointPayload{}, errors.New("checkpoint checksum mismatch")
	}

	var wire checkpointWirePayload
	if err := msgpack.Unmarshal(payloadBytes, &wire); err != nil {
		return checkpointPayload{}, fmt.Errorf("decode binary checkpoint payload: %w", err)
	}
	payload, err := decodeCheckpointPayload(wire)
	if err != nil {
		return checkpointPayload{}, err
	}
	return payload, nil
}

func unmarshalLegacyCheckpoint(data []byte) (checkpointPayload, error) {
	var envelope checkpointEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return checkpointPayload{}, fmt.Errorf("decode checkpoint: %w", err)
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.Checksum != hex.EncodeToString(digest[:]) {
		return checkpointPayload{}, errors.New("checkpoint checksum mismatch")
	}
	var payload checkpointPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return checkpointPayload{}, fmt.Errorf("decode checkpoint payload: %w", err)
	}
	return payload, nil
}

func encodeCheckpointPayload(payload checkpointPayload) checkpointWirePayload {
	strings := newCheckpointStringTable(payload)
	wire := checkpointWirePayload{
		CreatedAtUnixNano:        payload.CreatedAt.UnixNano(),
		ConfigurationFingerprint: payload.ConfigurationFingerprint,
		Strings:                  strings.values,
		Entries:                  make([]checkpointWireEntry, 0, len(payload.Index.Entries)),
		EngineMappings:           make([]checkpointWireEngineMapping, 0, len(payload.Index.EngineMappings)),
		Dedup:                    make([]checkpointWireDedupEntry, 0, len(payload.Dedup)),
		Groups:                   make([]checkpointWireGroupEntry, 0, len(payload.Groups)),
		LastConsumedSequences:    make([]checkpointWireSequence, 0, len(payload.LastConsumedSequences)),
	}
	for _, entry := range payload.Index.Entries {
		pods := make([]checkpointWirePod, 0, len(entry.Pods))
		for _, pod := range entry.Pods {
			var flags uint8
			if pod.Speculative {
				flags |= wirePodSpeculative
			}
			if pod.HasGroup {
				flags |= wirePodHasGroup
			}
			pods = append(pods, checkpointWirePod{
				PodIdentifier: strings.index[pod.PodIdentifier],
				DeviceTier:    strings.index[pod.DeviceTier],
				Flags:         flags,
				GroupID:       int64(pod.GroupIdx),
			})
		}
		wire.Entries = append(wire.Entries, checkpointWireEntry{
			RequestKey: uint64(entry.RequestKey),
			Pods:       pods,
		})
	}
	for _, mapping := range payload.Index.EngineMappings {
		requestKeys := make([]uint64, len(mapping.RequestKeys))
		for i, requestKey := range mapping.RequestKeys {
			requestKeys[i] = uint64(requestKey)
		}
		wire.EngineMappings = append(wire.EngineMappings, checkpointWireEngineMapping{
			EngineKey: uint64(mapping.EngineKey), RequestKeys: requestKeys,
		})
	}
	for _, entry := range payload.Dedup {
		wire.Dedup = append(wire.Dedup, checkpointWireDedupEntry{
			PodIdentifier:    strings.index[entry.PodIdentifier],
			DeviceTier:       strings.index[entry.DeviceTier],
			GroupIdx:         int64(entry.GroupIdx),
			DataParallelRank: int64(entry.DataParallelRank),
			BlockHash:        entry.BlockHash,
			Count:            uint64(entry.Count),
		})
	}
	for _, entry := range payload.Groups {
		slidingWindowSize := int64(-1)
		if entry.Metadata.SlidingWindowSize != nil {
			slidingWindowSize = int64(*entry.Metadata.SlidingWindowSize)
		}
		wire.Groups = append(wire.Groups, checkpointWireGroupEntry{
			PodIdentifier:     strings.index[entry.PodIdentifier],
			GroupID:           int64(entry.GroupID),
			Kind:              strings.index[entry.Metadata.Kind],
			BlockSize:         uint64(entry.Metadata.BlockSize),
			SlidingWindowSize: slidingWindowSize,
		})
	}
	sources := make([]string, 0, len(payload.LastConsumedSequences))
	for source := range payload.LastConsumedSequences {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	for _, source := range sources {
		wire.LastConsumedSequences = append(wire.LastConsumedSequences, checkpointWireSequence{
			Source: strings.index[source], Sequence: payload.LastConsumedSequences[source],
		})
	}
	return wire
}

func decodeCheckpointPayload(wire checkpointWirePayload) (checkpointPayload, error) {
	stringAt := func(index uint32) (string, error) {
		if uint64(index) >= uint64(len(wire.Strings)) {
			return "", fmt.Errorf("checkpoint string index %d is out of range", index)
		}
		return wire.Strings[index], nil
	}
	payload := checkpointPayload{
		SchemaVersion:            checkpointSchemaVersion,
		CreatedAt:                time.Unix(0, wire.CreatedAtUnixNano).UTC(),
		ConfigurationFingerprint: wire.ConfigurationFingerprint,
		Index: kvblock.IndexSnapshot{
			Entries:        make([]kvblock.IndexSnapshotEntry, 0, len(wire.Entries)),
			EngineMappings: make([]kvblock.EngineMappingSnapshot, 0, len(wire.EngineMappings)),
		},
		Dedup:                 make([]dedupSnapshotEntry, 0, len(wire.Dedup)),
		Groups:                make([]kvblock.GroupCatalogSnapshotEntry, 0, len(wire.Groups)),
		LastConsumedSequences: make(map[string]uint64, len(wire.LastConsumedSequences)),
	}
	for _, entry := range wire.Entries {
		pods := make([]kvblock.PodEntry, 0, len(entry.Pods))
		for _, pod := range entry.Pods {
			podIdentifier, err := stringAt(pod.PodIdentifier)
			if err != nil {
				return checkpointPayload{}, err
			}
			deviceTier, err := stringAt(pod.DeviceTier)
			if err != nil {
				return checkpointPayload{}, err
			}
			pods = append(pods, kvblock.PodEntry{
				PodIdentifier: podIdentifier,
				DeviceTier:    deviceTier,
				Speculative:   pod.Flags&wirePodSpeculative != 0,
				HasGroup:      pod.Flags&wirePodHasGroup != 0,
				GroupIdx:      kvblock.GroupID(pod.GroupID),
			})
		}
		payload.Index.Entries = append(payload.Index.Entries, kvblock.IndexSnapshotEntry{
			RequestKey: kvblock.BlockHash(entry.RequestKey), Pods: pods,
		})
	}
	for _, mapping := range wire.EngineMappings {
		requestKeys := make([]kvblock.BlockHash, len(mapping.RequestKeys))
		for i, requestKey := range mapping.RequestKeys {
			requestKeys[i] = kvblock.BlockHash(requestKey)
		}
		payload.Index.EngineMappings = append(payload.Index.EngineMappings, kvblock.EngineMappingSnapshot{
			EngineKey: kvblock.BlockHash(mapping.EngineKey), RequestKeys: requestKeys,
		})
	}
	for _, entry := range wire.Dedup {
		podIdentifier, err := stringAt(entry.PodIdentifier)
		if err != nil {
			return checkpointPayload{}, err
		}
		deviceTier, err := stringAt(entry.DeviceTier)
		if err != nil {
			return checkpointPayload{}, err
		}
		payload.Dedup = append(payload.Dedup, dedupSnapshotEntry{
			PodIdentifier:    podIdentifier,
			DeviceTier:       deviceTier,
			GroupIdx:         int(entry.GroupIdx),
			DataParallelRank: int(entry.DataParallelRank),
			BlockHash:        entry.BlockHash,
			Count:            int(entry.Count),
		})
	}
	for _, entry := range wire.Groups {
		podIdentifier, err := stringAt(entry.PodIdentifier)
		if err != nil {
			return checkpointPayload{}, err
		}
		kind, err := stringAt(entry.Kind)
		if err != nil {
			return checkpointPayload{}, err
		}
		var slidingWindowSize *int
		if entry.SlidingWindowSize >= 0 {
			value := int(entry.SlidingWindowSize)
			slidingWindowSize = &value
		}
		payload.Groups = append(payload.Groups, kvblock.GroupCatalogSnapshotEntry{
			PodIdentifier: podIdentifier,
			GroupID:       kvblock.GroupID(entry.GroupID),
			Metadata: kvblock.GroupMetadata{
				Kind: kind, BlockSize: int(entry.BlockSize), SlidingWindowSize: slidingWindowSize,
			},
		})
	}
	for _, entry := range wire.LastConsumedSequences {
		source, err := stringAt(entry.Source)
		if err != nil {
			return checkpointPayload{}, err
		}
		if _, exists := payload.LastConsumedSequences[source]; exists {
			return checkpointPayload{}, fmt.Errorf("checkpoint contains duplicate event source %q", source)
		}
		payload.LastConsumedSequences[source] = entry.Sequence
	}
	return payload, nil
}

func newCheckpointStringTable(payload checkpointPayload) checkpointStringTable {
	unique := make(map[string]struct{})
	for _, entry := range payload.Index.Entries {
		for _, pod := range entry.Pods {
			unique[pod.PodIdentifier] = struct{}{}
			unique[pod.DeviceTier] = struct{}{}
		}
	}
	for _, entry := range payload.Dedup {
		unique[entry.PodIdentifier] = struct{}{}
		unique[entry.DeviceTier] = struct{}{}
	}
	for _, entry := range payload.Groups {
		unique[entry.PodIdentifier] = struct{}{}
		unique[entry.Metadata.Kind] = struct{}{}
	}
	for source := range payload.LastConsumedSequences {
		unique[source] = struct{}{}
	}
	values := make([]string, 0, len(unique))
	for value := range unique {
		values = append(values, value)
	}
	sort.Strings(values)
	index := make(map[string]uint32, len(values))
	for i, value := range values {
		index[value] = uint32(i)
	}
	return checkpointStringTable{values: values, index: index}
}
