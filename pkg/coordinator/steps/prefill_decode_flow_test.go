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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/coordinator/config"
	"github.com/llm-d/llm-d-router/pkg/coordinator/connectors/kv"
	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

func TestPrefillPromptTokenReuse_ChatRoundTrip(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonstreaming", true: "streaming"}[stream], func(t *testing.T) {
			original := map[string]any{
				"model": "glm", "stream": stream, "max_completion_tokens": float64(128),
				"temperature": 0.2, "cache_salt": "tenant-salt",
				"messages":    []any{map[string]any{"role": "user", "content": "Use the weather tool"}},
				"tools":       []any{map[string]any{"type": "function", "function": map[string]any{"name": "weather"}}},
				"tool_choice": "auto", "return_token_ids": false,
			}
			if stream {
				original["messages"] = []any{
					map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "Use the weather tool"}}},
					map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "earlier-call", "type": "function", "function": map[string]any{"name": "weather", "arguments": "{}"}}}},
					map[string]any{"role": "tool", "tool_call_id": "earlier-call", "content": "sunny"},
				}
			}
			promptIDs := []any{float64(0), float64(100), float64(200)}
			response := `{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`
			if stream {
				response = "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"checking\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"weather\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"
			}
			var phases []string
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != reqcommon.PathChatCompletions {
					t.Errorf("path = %q, want chat completions", r.URL.Path)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				phase := r.Header.Get(gateway.EPPProfileHeader)
				phases = append(phases, phase)
				switch phase {
				case gateway.PhasePrefill:
					if body["return_token_ids"] != true || body["stream"] != false || body["max_completion_tokens"] != float64(1) {
						t.Errorf("prefill must request prompt IDs with one nonstreaming output token: %v", body)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"prompt_token_ids": promptIDs,
						"choices":          []any{map[string]any{"token_ids": []int{999}}},
						"kv_transfer_params": map[string]any{
							"remote_request_id": "prefill-request", "remote_engine_id": "prefill-engine",
							"remote_block_ids": []any{[]any{float64(4), float64(5)}},
						},
					})
				case gateway.PhaseDecode:
					params, ok := body["kv_transfer_params"].(map[string]any)
					if !ok || !reflect.DeepEqual(params["prompt_token_ids"], promptIDs) {
						t.Errorf("decode prompt IDs = %v, want %v without P's sampled token", params["prompt_token_ids"], promptIDs)
					}
					if params["remote_request_id"] != "prefill-request" || params["remote_engine_id"] != "prefill-engine" || params["do_remote_prefill"] != true {
						t.Errorf("decode lost KV handoff: %v", params)
					}
					for key, want := range original {
						if !reflect.DeepEqual(body[key], want) {
							t.Errorf("decode %s = %v, want %v", key, body[key], want)
						}
					}
					w.Header().Set("Content-Type", map[bool]string{false: "application/json", true: "text/event-stream"}[stream])
					_, _ = io.WriteString(w, response)
				default:
					t.Errorf("unexpected phase %q", phase)
				}
			}))
			defer backend.Close()
			client := gateway.New(config.GatewayConfig{Address: backend.URL})
			prefill, err := NewPrefillStep(client, map[string]any{ParamKVConnector: kv.NIXL, "reuse_prompt_token_ids": true})
			if err != nil {
				t.Fatal(err)
			}
			decode, err := NewDecodeStep(client, map[string]any{ParamKVConnector: kv.NIXL})
			if err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			bodyBytes, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			if err := json.Unmarshal(bodyBytes, &body); err != nil {
				t.Fatal(err)
			}
			reqCtx := &pipeline.RequestContext{
				RequestID: "request-1", OriginalPath: reqcommon.PathChatCompletions,
				Body: body, Model: "glm", Stream: stream, ResponseWriter: recorder,
			}
			for _, step := range []pipeline.Step{prefill, decode} {
				if err := step.Execute(context.Background(), reqCtx); err != nil {
					t.Fatalf("%s: %v", step.Name(), err)
				}
			}
			if !reflect.DeepEqual(phases, []string{gateway.PhasePrefill, gateway.PhaseDecode}) {
				t.Errorf("phases = %v, want one P and one D request", phases)
			}
			if got := recorder.Body.String(); got != response {
				t.Errorf("chat response changed: got %q, want %q", got, response)
			}
		})
	}
}

// TestECTransferParams_NotForwardedToDecodeBackend verifies that ec_transfer_params
// accumulated by the encode step are never included in the decode request body.
// The prefill step uses a copy of reqCtx.Body, so it cannot contaminate the shared
// body map; this test guards against regressions where that isolation breaks.
func TestECTransferParams_NotForwardedToDecodeBackend(t *testing.T) {
	var decodeBody map[string]any

	gwServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(gateway.EPPProfileHeader) == gateway.PhaseDecode {
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &decodeBody)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"role": "assistant", "content": "ok"}},
			},
		})
	}))
	defer gwServer.Close()

	gwClient := gateway.New(config.GatewayConfig{Address: gwServer.URL})
	decodeStep, _ := NewDecodeStep(gwClient, map[string]any{ParamKVConnector: kv.NIXL})

	reqCtx := &pipeline.RequestContext{
		RequestID:    "test-no-ec",
		OriginalPath: reqcommon.PathChatCompletions,
		Model:        "llama-3",
		Stream:       false,
		// Simulate encode step having populated ECTransferParams.
		ECTransferParams: []map[string]any{
			{"img-hash-1": map[string]any{"peer_host": "10.0.0.5", "peer_port": float64(5500)}},
		},
		KVTransferParams: map[string]any{"block_id": "blk-1"},
		Body: map[string]any{
			"model":    "llama-3",
			"messages": []any{map[string]any{"role": "user", "content": "hello"}},
		},
	}

	recorder := httptest.NewRecorder()
	reqCtx.ResponseWriter = recorder

	if err := decodeStep.Execute(context.Background(), reqCtx); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decodeBody == nil {
		t.Fatal("decode step did not reach the backend")
	}
	if _, present := decodeBody["ec_transfer_params"]; present {
		t.Errorf("ec_transfer_params must not be forwarded to the decode backend, got: %v", decodeBody["ec_transfer_params"])
	}
}

func TestKVTransferParams_FlowFromPrefillToDecode(t *testing.T) {
	expectedKVParams := map[string]any{
		"block_id":  "block-999",
		"peer_host": "10.0.0.42",
		"peer_port": float64(7777),
	}

	var decodeReceivedKVParams map[string]any

	gwServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		phase := r.Header.Get(gateway.EPPProfileHeader)
		switch phase {
		case gateway.PhasePrefill:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"kv_transfer_params": expectedKVParams,
			})

		case gateway.PhaseDecode:
			body, _ := io.ReadAll(r.Body)
			var parsed map[string]any
			_ = json.Unmarshal(body, &parsed)
			decodeReceivedKVParams, _ = parsed["kv_transfer_params"].(map[string]any)

			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{
					{"message": map[string]any{"role": "assistant", "content": "done"}},
				},
			})

		default:
			http.Error(w, "not found", 404)
		}
	}))
	defer gwServer.Close()

	gwClient := gateway.New(config.GatewayConfig{Address: gwServer.URL})

	// Run prefill step
	prefillStep, _ := NewPrefillStep(gwClient, map[string]any{ParamKVConnector: kv.NIXL})

	reqCtx := &pipeline.RequestContext{
		RequestID:    "test-flow",
		OriginalPath: reqcommon.PathChatCompletions,
		Model:        "llama-3",
		Stream:       false,
		TokenIDs:     []int{1, 32000, 32000, 32000, 2345},
		MultimodalEntries: []pipeline.MultimodalEntry{
			{Index: 0, Hash: "hash-1", Placeholder: pipeline.PlaceholderRange{Offset: 1, Length: 3}},
		},
		ECTransferParams: []map[string]any{
			{"hash-1": map[string]any{"peer_host": "10.0.0.1", "peer_port": 5501}},
		},
		KVTransferParams: make(map[string]any),
		Body: map[string]any{
			"model":  "llama-3",
			"stream": false,
			"messages": []any{
				map[string]any{
					"role": "user",
					"content": []any{
						map[string]any{
							"type":      "image_url",
							"image_url": map[string]any{"url": "https://example.com/img.jpg"},
						},
					},
				},
			},
		},
	}

	err := prefillStep.Execute(context.Background(), reqCtx)
	if err != nil {
		t.Fatalf("prefill failed: %v", err)
	}

	if reqCtx.KVTransferParams["block_id"] != "block-999" {
		t.Fatalf("prefill did not set kv_transfer_params correctly: %v", reqCtx.KVTransferParams)
	}

	// Run decode step
	decodeStep, _ := NewDecodeStep(gwClient, map[string]any{ParamKVConnector: kv.NIXL})

	recorder := httptest.NewRecorder()
	reqCtx.ResponseWriter = recorder

	err = decodeStep.Execute(context.Background(), reqCtx)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decodeReceivedKVParams == nil {
		t.Fatal("decode did not send kv_transfer_params to gateway")
	}
	if decodeReceivedKVParams["block_id"] != "block-999" {
		t.Fatalf("decode sent wrong block_id: %v", decodeReceivedKVParams["block_id"])
	}
	if decodeReceivedKVParams["peer_host"] != "10.0.0.42" {
		t.Fatalf("decode sent wrong peer_host: %v", decodeReceivedKVParams["peer_host"])
	}
}
