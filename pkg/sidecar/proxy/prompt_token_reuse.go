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

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

const requestFieldEcho = "echo"

// isTextChatForTokenReuse reports whether decode can reuse prefill's prompt
// IDs. Token IDs alone cannot describe the multimodal state decode needs, and
// vLLM renders echo requests regardless, so both keep their regular path.
func isTextChatForTokenReuse(body map[string]any) bool {
	if requestFlag(body, requestFieldEcho) {
		return false
	}
	messages, err := requestMessages(body)
	if err != nil || len(messages) == 0 {
		return false
	}
	for _, raw := range messages {
		var message map[string]any
		if err := json.Unmarshal(raw, &message); err != nil || message == nil || message["audio"] != nil {
			return false
		}
		switch content := message["content"].(type) {
		case nil, string:
		case []any:
			for _, rawPart := range content {
				part, ok := rawPart.(map[string]any)
				if !ok || (part["type"] != "text" && part["type"] != "refusal") {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

// requestFlag reads a boolean top-level field that decodeRequestBody may have
// left as raw JSON.
func requestFlag(body map[string]any, field string) bool {
	switch v := body[field].(type) {
	case bool:
		return v
	case json.RawMessage:
		var flag bool
		return json.Unmarshal(v, &flag) == nil && flag
	default:
		return false
	}
}

// attachPromptTokenIDs copies prefill's prompt IDs into the kv_transfer_params
// sent to decode. On error nothing is attached, so decode renders and
// tokenizes the prompt itself, as it does with reuse disabled.
func attachPromptTokenIDs(response map[string]any, transferParams any) error {
	kv, ok := transferParams.(map[string]any)
	if !ok || len(kv) == 0 {
		return errors.New("prefill returned no kv_transfer_params")
	}
	ids, ok := response["prompt_token_ids"].([]any)
	if !ok || len(ids) == 0 {
		return errors.New("prefill returned no prompt_token_ids")
	}
	for i, raw := range ids {
		id, ok := raw.(float64)
		if !ok || id < 0 || id > math.MaxUint32 || id != math.Trunc(id) {
			return fmt.Errorf("prefill returned invalid prompt token ID at index %d", i)
		}
	}
	kv["prompt_token_ids"] = ids
	return nil
}
