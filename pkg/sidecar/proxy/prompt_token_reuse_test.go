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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/common/routing"
	"github.com/llm-d/llm-d-router/pkg/sidecar/constants"
)

func TestNIXLPromptTokenReuse(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			messages string
			enabled  bool
			prefill  bool
			reuse    bool
		}{
			{"text", `[{"role":"user","content":"Hello"}]`, true, true, true},
			{"text parts and tools", `[{"role":"user","content":[{"type":"text","text":"Hello"}]},{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_1","content":"done"}]`, true, true, true},
			{"image", `[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.org/image.png"}}]}]`, true, true, false},
			{"video", `[{"role":"user","content":[{"type":"video_url","video_url":{"url":"https://example.org/video.mp4"}}]}]`, true, true, false},
			{"audio", `[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AA==","format":"wav"}}]}]`, true, true, false},
			{"assistant audio", `[{"role":"assistant","content":null,"audio":{"id":"audio_1"}}]`, true, true, false},
			{"unknown content", `[{"role":"user","content":[{"type":"future_media"}]}]`, true, true, false},
			{"batch", `[[{"role":"user","content":"Hello"}]]`, true, true, false},
			{"disabled", `[{"role":"user","content":"Hello"}]`, false, true, false},
			{"local prefill", `[{"role":"user","content":"Hello"}]`, true, false, false},
		} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				prefillRequests := make(chan map[string]any, 1)
				decodeRequests := make(chan map[string]any, 1)
				decodeRawRequests := make(chan string, 1)
				prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					prefillRequests <- body
					_, _ = io.WriteString(w, `{"prompt_token_ids":[1,42,1000000,4294967295],"choices":[{"token_ids":[123]}],"kv_transfer_params":{"do_remote_prefill":true,"remote_block_ids":[[7,8]],"remote_engine_id":"prefill"}}`)
				}))
				defer prefill.Close()
				response := `{"choices":[{"message":{"role":"assistant","content":"Hello"}}]}`
				if stream {
					response = "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"Thinking\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]}}]}\n\ndata: [DONE]\n\n"
				}
				decode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, err := io.ReadAll(r.Body)
					var body map[string]any
					if err == nil {
						err = json.Unmarshal(raw, &body)
					}
					if err != nil {
						t.Error(err)
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					decodeRequests <- body
					decodeRawRequests <- string(raw)
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
					}
					_, _ = io.WriteString(w, response)
				}))
				defer decode.Close()
				decodeURL, err := url.Parse(decode.URL)
				require.NoError(t, err)
				proxy := NewProxy(Config{DecoderURL: decodeURL, KVConnector: constants.KVConnectorNIXLV2, ReusePromptTokenIDs: tc.enabled})
				proxy.allowlistValidator = &AllowlistValidator{enabled: false}
				requestBody := fmt.Sprintf(`{"model":"test","messages":%s,"max_tokens":50,"stream":%t,"return_token_ids":false,"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]}`, tc.messages, stream)
				r := httptest.NewRequest(http.MethodPost, reqcommon.PathChatCompletions, strings.NewReader(requestBody))
				if tc.prefill {
					r.Header.Set(routing.PrefillEndpointHeader, strings.TrimPrefix(prefill.URL, "http://"))
				}
				w := httptest.NewRecorder()
				proxy.createRoutes().ServeHTTP(w, r)
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Equal(t, response, w.Body.String())
				var original map[string]any
				require.NoError(t, json.Unmarshal([]byte(requestBody), &original))
				require.Len(t, decodeRequests, 1)
				d := <-decodeRequests
				dRaw := <-decodeRawRequests
				if tc.prefill {
					require.Len(t, prefillRequests, 1)
					p := <-prefillRequests
					require.Equal(t, tc.reuse, p["return_token_ids"])
					require.Equal(t, false, p["stream"])
					require.Equal(t, float64(1), p["max_tokens"])
					require.Equal(t, original["messages"], p["messages"])
					kv := d["kv_transfer_params"].(map[string]any)
					require.Equal(t, "prefill", kv["remote_engine_id"])
					require.Equal(t, []any{[]any{float64(7), float64(8)}}, kv["remote_block_ids"])
					if tc.reuse {
						require.Equal(t, []any{float64(1), float64(42), float64(1000000), float64(4294967295)}, kv["prompt_token_ids"])
						// vLLM rejects non-integer IDs, so large IDs must not use exponent notation.
						require.Contains(t, dRaw, `"prompt_token_ids":[1,42,1000000,4294967295]`)
					} else {
						require.NotContains(t, kv, "prompt_token_ids")
					}
					delete(d, "kv_transfer_params")
				} else {
					require.Empty(t, prefillRequests)
				}
				require.Equal(t, original, d)
			})
		}
	}
}

func TestNIXLPromptTokenReuseInvalidResponseFallsBack(t *testing.T) {
	for _, response := range []string{
		`{"kv_transfer_params":{"do_remote_prefill":true}}`,
		`{"prompt_token_ids":[],"kv_transfer_params":{"do_remote_prefill":true}}`,
		`{"prompt_token_ids":[1,null],"kv_transfer_params":{"do_remote_prefill":true}}`,
		`{"prompt_token_ids":[-1],"kv_transfer_params":{"do_remote_prefill":true}}`,
		`{"prompt_token_ids":[1.5],"kv_transfer_params":{"do_remote_prefill":true}}`,
		`{"prompt_token_ids":[4294967296],"kv_transfer_params":{"do_remote_prefill":true}}`,
		`{"prompt_token_ids":["42"],"kv_transfer_params":{"do_remote_prefill":true}}`,
		`{"prompt_token_ids":[1],"kv_transfer_params":{}}`,
		`{"prompt_token_ids":[1],"kv_transfer_params":null}`,
		`{"prompt_token_ids":[1]}`,
	} {
		t.Run(response, func(t *testing.T) {
			prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, response)
			}))
			defer prefill.Close()
			var prefillResponse map[string]any
			require.NoError(t, json.Unmarshal([]byte(response), &prefillResponse))
			decodeURL, err := url.Parse("http://127.0.0.1:1")
			require.NoError(t, err)
			proxy := NewProxy(Config{DecoderURL: decodeURL, KVConnector: constants.KVConnectorNIXLV2, ReusePromptTokenIDs: true})
			var decodeBody map[string]any
			proxy.decoderProxy = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&decodeBody); err != nil {
					t.Error(err)
				}
				_, _ = io.WriteString(w, `{"choices":[]}`)
			})
			r := httptest.NewRequest(http.MethodPost, reqcommon.PathChatCompletions, strings.NewReader(`{"messages":[{"role":"user","content":"Hello"}]}`))
			w := httptest.NewRecorder()
			proxy.handleNIXLV2(w, r, strings.TrimPrefix(prefill.URL, "http://"), "", reqcommon.APITypeChatCompletions)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.NotNil(t, decodeBody, "decode must run without reusable prompt token IDs")
			require.Equal(t, prefillResponse["kv_transfer_params"], decodeBody["kv_transfer_params"])
		})
	}
}

func TestIsTextChatForTokenReuseEcho(t *testing.T) {
	for _, tc := range []struct {
		body  string
		reuse bool
	}{
		{`{"messages":[{"role":"user","content":"Hello"}],"echo":true}`, false},
		{`{"messages":[{"role":"user","content":"Hello"}],"echo":false}`, true},
		{`{"messages":[{"role":"user","content":"Hello"}],"echo":null}`, true},
	} {
		t.Run(tc.body, func(t *testing.T) {
			body, err := decodeRequestBody([]byte(tc.body))
			require.NoError(t, err)
			require.Equal(t, tc.reuse, isTextChatForTokenReuse(body))
		})
	}
}

func TestPromptTokenReuseOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		on   bool
		err  string
	}{
		{"default", nil, false, ""},
		{"flag", []string{"--reuse-prompt-token-ids"}, true, ""},
		{"yaml", []string{"--configuration=reuse-prompt-token-ids: true"}, true, ""},
		{"flag overrides yaml", []string{"--configuration=reuse-prompt-token-ids: true", "--reuse-prompt-token-ids=false"}, false, ""},
		{"connector", []string{"--reuse-prompt-token-ids", "--kv-connector=sglang"}, true, "requires --kv-connector=nixlv2"},
		{"parallel", []string{"--reuse-prompt-token-ids", "--moriio-write-mode", "--moriio-local-pod-ip=127.0.0.1", "--moriio-parallel-dispatch"}, true, "parallel dispatch"},
		{"chunked", []string{"--reuse-prompt-token-ids", "--decode-chunk-size=64"}, true, "chunked decode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := NewOptions()
			fs := pflag.NewFlagSet(tc.name, pflag.ContinueOnError)
			opts.AddFlags(fs)
			require.NoError(t, fs.Parse(tc.args))
			require.NoError(t, opts.Complete())
			require.Equal(t, tc.on, opts.ReusePromptTokenIDs)
			if tc.err != "" {
				require.ErrorContains(t, opts.Validate(), tc.err)
			} else {
				require.NoError(t, opts.Validate())
			}
		})
	}
}
