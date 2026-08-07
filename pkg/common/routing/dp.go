// Copyright 2025 The llm-d Authors.
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

//revive:disable:var-naming
package routing

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// NoDataParallelRank is the sentinel rank for a non-data-parallel engine.
	// Real ranks are always non-negative.
	NoDataParallelRank = -1

	// DPRankSuffix separates a pod identifier from its data-parallel rank in a
	// scoring key: "<podIdentifier>@dp<rank>".
	DPRankSuffix = "@dp"

	// DataParallelRankHeader pins a request to one data-parallel rank inside a
	// vLLM server running Internal or Hybrid LB, bypassing its internal
	// queue-based balancing. Unlike DataParallelEndpointHeader this addresses a
	// rank behind a shared HTTP port rather than a rank-specific port, so it is
	// the only way to steer when the ranks are not separate endpoints. The
	// routing sidecar already sets this same header on its prefill leg.
	DataParallelRankHeader = "x-data-parallel-rank"

	// DataParallelWinningRanksHeader carries per-pod winning ranks from
	// scheduling to the pre-request stage as a JSON object of pod address to
	// rank, e.g. {"10.0.0.1:8000":0}. It is internal to the EPP and is removed
	// before the request is forwarded upstream.
	DataParallelWinningRanksHeader = "x-llm-d-dp-winning-ranks"
)

// ErrEmptyWinningRanks reports that there are no winning ranks to transport.
// Callers should skip emitting the header rather than send an empty object.
var ErrEmptyWinningRanks = errors.New("winning ranks map is empty")

// ParseDPScoringKey splits a scoring key into its pod identifier and
// data-parallel rank, returning NoDataParallelRank when the key carries none.
//
// Only a suffix of the exact shape "@dp<digits>" is treated as a rank, so a pod
// identifier that happens to contain "@dp" (for instance "pod@dp-service:8080")
// is returned unchanged rather than being mis-split. The last such suffix wins,
// which matters for identifiers that contain both.
func ParseDPScoringKey(scoringKey string) (podIdentifier string, dataParallelRank int) {
	idx := strings.LastIndex(scoringKey, DPRankSuffix)
	if idx < 0 {
		return scoringKey, NoDataParallelRank
	}

	rank, err := strconv.Atoi(scoringKey[idx+len(DPRankSuffix):])
	// Atoi accepts a leading sign, which BuildDPScoringKey never emits, so
	// reject anything negative alongside the parse error.
	if err != nil || rank < 0 {
		return scoringKey, NoDataParallelRank
	}
	// Reject "+3" and "007", which Atoi accepts but are not round-trippable.
	if strconv.Itoa(rank) != scoringKey[idx+len(DPRankSuffix):] {
		return scoringKey, NoDataParallelRank
	}

	return scoringKey[:idx], rank
}

// BuildDPScoringKey composes a scoring key from a pod identifier and rank,
// returning the bare identifier for NoDataParallelRank. A negative rank other
// than the sentinel is rejected rather than encoded, since ParseDPScoringKey
// would refuse to read it back and the round-trip would silently lose it.
func BuildDPScoringKey(podIdentifier string, dataParallelRank int) (string, error) {
	if dataParallelRank == NoDataParallelRank {
		return podIdentifier, nil
	}
	if dataParallelRank < 0 {
		return "", fmt.Errorf("invalid negative data-parallel rank %d", dataParallelRank)
	}
	return podIdentifier + DPRankSuffix + strconv.Itoa(dataParallelRank), nil
}

// StripDPRankSuffix returns the pod identifier of a scoring key, dropping any
// data-parallel rank.
func StripDPRankSuffix(scoringKey string) string {
	podIdentifier, _ := ParseDPScoringKey(scoringKey)
	return podIdentifier
}

// EncodeWinningRanks serialises a pod-address to rank map for header
// transport. It returns ErrEmptyWinningRanks for an empty map and rejects
// negative ranks.
func EncodeWinningRanks(ranks map[string]int) (string, error) {
	if len(ranks) == 0 {
		return "", ErrEmptyWinningRanks
	}
	for pod, rank := range ranks {
		if rank < 0 {
			return "", fmt.Errorf("invalid negative data-parallel rank %d for pod %q", rank, pod)
		}
	}
	encoded, err := json.Marshal(ranks)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// DecodeWinningRanks reverses EncodeWinningRanks. It returns
// ErrEmptyWinningRanks for empty input and rejects negative ranks.
func DecodeWinningRanks(encoded string) (map[string]int, error) {
	if encoded == "" {
		return nil, ErrEmptyWinningRanks
	}
	var ranks map[string]int
	if err := json.Unmarshal([]byte(encoded), &ranks); err != nil {
		return nil, err
	}
	if len(ranks) == 0 {
		return nil, ErrEmptyWinningRanks
	}
	for pod, rank := range ranks {
		if rank < 0 {
			return nil, fmt.Errorf("invalid negative data-parallel rank %d for pod %q", rank, pod)
		}
	}
	return ranks, nil
}
