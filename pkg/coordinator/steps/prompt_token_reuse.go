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

package steps

import (
	"fmt"

	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

// Token IDs alone cannot describe the multimodal inputs D needs.
func validateTextChatForTokenReuse(body map[string]any) error {
	messages, ok := body["messages"].([]any)
	if !ok {
		return fmt.Errorf("reuse_prompt_token_ids requires a text messages array: %w", pipeline.ErrBadRequest)
	}
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("reuse_prompt_token_ids requires text chat messages: %w", pipeline.ErrBadRequest)
		}
		switch content := message["content"].(type) {
		case nil, string:
		case []any:
			for _, rawPart := range content {
				part, ok := rawPart.(map[string]any)
				if !ok || (part["type"] != "text" && part["type"] != "refusal") {
					return fmt.Errorf("reuse_prompt_token_ids supports only text chat content: %w", pipeline.ErrBadRequest)
				}
			}
		default:
			return fmt.Errorf("reuse_prompt_token_ids requires text chat content: %w", pipeline.ErrBadRequest)
		}
	}
	return nil
}
