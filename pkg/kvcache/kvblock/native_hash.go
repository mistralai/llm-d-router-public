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
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// NativeBlockKey scopes an engine hash by its base model and cache group.
// Nil group identifies legacy events that do not carry a group index.
func NativeBlockKey(model string, group *int, hash uint64) BlockHash {
	var data [24]byte
	binary.LittleEndian.PutUint64(data[:8], xxhash.Sum64String(model))
	groupID := uint64(0)
	if group != nil {
		groupID = uint64(*group) + 1 // #nosec G115 -- group indices are nonnegative.
	}
	binary.LittleEndian.PutUint64(data[8:16], groupID)
	binary.LittleEndian.PutUint64(data[16:], hash)
	return BlockHash(xxhash.Sum64(data[:]))
}
