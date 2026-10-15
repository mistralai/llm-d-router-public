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

package proxy

import "time"

const mistralMaxHeaderBytes = 16 << 10

type mistralLimits struct {
	MaxRequestBytes  int64
	MaxMetadataBytes int64
	MaxParts         int
	MaxInFlight      int
	MaxEventBytes    int
	MaxPrefillBytes  int64
	PrefillTimeout   time.Duration
}

func defaultMistralLimits() mistralLimits {
	return mistralLimits{
		MaxRequestBytes: 256 << 20, MaxMetadataBytes: 8 << 20, MaxParts: 128,
		MaxInFlight: 8, MaxEventBytes: 4 << 20, MaxPrefillBytes: 16 << 20,
		PrefillTimeout: 120 * time.Second,
	}
}
