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

package routing

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// NoDataParallelRank identifies an endpoint without a logical DP rank.
	NoDataParallelRank = -1
	// DPRankSuffix separates a serving endpoint identifier from its logical rank.
	DPRankSuffix = "@dp"
	// DataParallelRankHeader pins a request to one rank behind a shared serving port.
	DataParallelRankHeader = "x-data-parallel-rank"
)

func BuildDPScoringKey(podIdentifier string, dataParallelRank int) (string, error) {
	if dataParallelRank == NoDataParallelRank {
		return podIdentifier, nil
	}
	if dataParallelRank < 0 {
		return "", fmt.Errorf("invalid negative data-parallel rank %d", dataParallelRank)
	}
	return podIdentifier + DPRankSuffix + strconv.Itoa(dataParallelRank), nil
}

func ParseDPScoringKey(scoringKey string) (string, int) {
	idx := strings.LastIndex(scoringKey, DPRankSuffix)
	if idx < 0 {
		return scoringKey, NoDataParallelRank
	}
	rankText := scoringKey[idx+len(DPRankSuffix):]
	rank, err := strconv.Atoi(rankText)
	if err != nil || rank < 0 || strconv.Itoa(rank) != rankText {
		return scoringKey, NoDataParallelRank
	}
	return scoringKey[:idx], rank
}
