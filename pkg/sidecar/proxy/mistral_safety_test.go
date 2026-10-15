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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/common/routing"
	"github.com/llm-d/llm-d-router/pkg/sidecar/metrics"
)

func nativeSafetyRequest(prefill string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/v2/completions", strings.NewReader(mistralTestRequest))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(routing.PrefillEndpointHeader, strings.TrimPrefix(prefill, "http://"))
	return request
}

func nativeSafetyDecode(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"token_ids\":[42],\"usage\":{\"num_cached_tokens\":3}}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)
	return server
}

func TestMistralTerminalEventStopsRead(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "done"}[success], func(t *testing.T) {
			upstreamCancelled := make(chan struct{})
			prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if success {
					_, _ = io.WriteString(w, "data: {\"kv_transfer_params\":"+mistralTestTransfer+"}\n\ndata: [DONE]\n\n")
				} else {
					_, _ = io.WriteString(w, "data: {\"error\":{\"code\":503,\"message\":\"busy\"}}\n\n")
				}
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(upstreamCancelled)
			}))
			defer prefill.Close()
			server := mistralTestProxy(t, nativeSafetyDecode(t), 0)
			request := nativeSafetyRequest(prefill.URL)
			ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
			defer cancel()
			request = request.WithContext(ctx)
			result := make(chan *httptest.ResponseRecorder, 1)
			handler := server.createRoutes()
			go func() { recorder := httptest.NewRecorder(); handler.ServeHTTP(recorder, request); result <- recorder }()
			select {
			case recorder := <-result:
				expected := 503
				if success {
					expected = 200
				}
				if recorder.Code != expected {
					t.Errorf("status=%d body=%s", recorder.Code, recorder.Body)
				}
			case <-time.After(time.Second):
				t.Fatal("terminal event waited for upstream EOF")
			}
			select {
			case <-upstreamCancelled:
			case <-time.After(time.Second):
				t.Fatal("upstream read was not stopped")
			}
		})
	}
}

func TestMistralRetryReleasesPreviousTransfer(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var attempts atomic.Int32
	prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == mistralCleanupPath {
			var body struct {
				IDs []string `json:"request_ids"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.IDs) != 1 {
				t.Errorf("cleanup IDs=%v", body.IDs)
			} else {
				order = append(order, "cleanup:"+body.IDs[0])
			}
			w.WriteHeader(200)
			return
		}
		attempt := attempts.Add(1)
		id := "first"
		if attempt > 1 {
			id = "second"
		}
		order = append(order, "prefill:"+id)
		descriptor := strings.ReplaceAll(mistralTestTransfer, "transfer-request", id)
		w.Header().Set("Content-Type", "text/event-stream")
		if attempt == 1 {
			_, _ = io.WriteString(w, "data: {\"kv_transfer_params\":"+descriptor+",\"error\":{\"code\":503,\"message\":\"retry\"}}\n\n")
		} else {
			_, _ = io.WriteString(w, "data: {\"kv_transfer_params\":"+descriptor+",\"usage\":{\"num_cached_tokens\":2}}\n\ndata: [DONE]\n\n")
		}
	}))
	defer prefill.Close()
	server := mistralTestProxy(t, nativeSafetyDecode(t), 2)
	recorder := httptest.NewRecorder()
	server.createRoutes().ServeHTTP(recorder, nativeSafetyRequest(prefill.URL))
	if recorder.Code != 200 || attempts.Load() != 2 {
		t.Fatalf("status=%d attempts=%d body=%s", recorder.Code, attempts.Load(), recorder.Body)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(order, ",") != "prefill:first,cleanup:first,prefill:second" {
		t.Errorf("order=%v", order)
	}
}

func TestMistralFailedDecodeReleasesTransfer(t *testing.T) {
	var released atomic.Int32
	prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == mistralCleanupPath {
			released.Add(1)
			w.WriteHeader(200)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"kv_transfer_params\":"+mistralTestTransfer+"}\n\ndata: [DONE]\n\n")
	}))
	defer prefill.Close()
	decode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		_, _ = io.WriteString(w, `{"error":"unavailable"}`)
	}))
	defer decode.Close()
	server := mistralTestProxy(t, decode, 0)
	recorder := httptest.NewRecorder()
	server.createRoutes().ServeHTTP(recorder, nativeSafetyRequest(prefill.URL))
	if recorder.Code != 503 || released.Load() != 1 {
		t.Errorf("status=%d releases=%d", recorder.Code, released.Load())
	}
}

func TestMistralMultilineDecodeUsage(t *testing.T) {
	recorder := httptest.NewRecorder()
	state := &mistralDecodeState{}
	writer, finish := newCachedTokensResponseWriterForAPIWithLimits(recorder, 2, true, reqcommon.APITypeMistralV3, defaultMistralLimits(), state)
	stream := []byte("id: sample\ndata: {\"usage\":\ndata: {\"num_cached_tokens\":3},\"token_ids\":[42]}\n\ndata: [DONE]\n\n")
	for _, b := range stream {
		if _, err := writer.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"num_cached_tokens":2`)) || !bytes.Contains(recorder.Body.Bytes(), []byte("id: sample\n")) {
		t.Errorf("body=%s", recorder.Body)
	}
}

func TestMistralBodyReadCloseIsIdempotent(t *testing.T) {
	body := &mistralRequestBody{reader: strings.NewReader("data"), request: &mistralRequest{}, final: true}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() { defer group.Done(); buffer := make([]byte, 1); _, _ = body.Read(buffer); _ = body.Close() }()
	}
	group.Wait()
	if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) && err == nil {
		t.Fatal("closed body stayed readable")
	}
}

func TestMistralDecodeTerminalStopsUpstream(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, terminal := range []string{"data: [DONE]\n\n", "data: {\"sequence_finish_reason\":\"abort\"}\n\n", "data: {\"error\":{\"code\":503}}\n\n"} {
			t.Run(fmt.Sprintf("parallel=%t/%s", parallel, terminal), func(t *testing.T) {
				var released atomic.Int32
				prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == mistralCleanupPath {
						released.Add(1)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"kv_transfer_params\":"+mistralTestTransfer+"}\n\ndata: [DONE]\n\n")
				}))
				defer prefill.Close()
				cancelled := make(chan struct{})
				decode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, terminal)
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					close(cancelled)
				}))
				defer decode.Close()
				server := mistralTestProxy(t, decode, 0)
				handler := server.createRoutes()
				request := nativeSafetyRequest(prefill.URL)
				if parallel {
					server.forwardDataParallel = true
					server.dataParallelProxies["rank:1"] = server.decoderProxy
					request.Header.Set(routing.DataParallelEndpointHeader, "rank:1")
				}
				ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
				defer cancel()
				errorsBefore := stageErrors(t, metrics.StageDecode)
				result := make(chan *httptest.ResponseRecorder, 1)
				go func() {
					recorder := httptest.NewRecorder()
					handler.ServeHTTP(recorder, request.WithContext(ctx))
					result <- recorder
				}()
				select {
				case recorder := <-result:
					if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), terminal) {
						t.Errorf("status=%d body=%s", recorder.Code, recorder.Body)
					}
					expected := int32(1)
					if strings.Contains(terminal, "[DONE]") {
						expected = 0
					}
					if released.Load() != expected {
						t.Errorf("cleanup count=%d want=%d", released.Load(), expected)
					}
					if delta := stageErrors(t, metrics.StageDecode) - errorsBefore; delta != float64(expected) {
						t.Errorf("decode error count increased by %v, want %d", delta, expected)
					}
				case <-time.After(time.Second):
					t.Error("decode terminal waited for EOF")
					cancel()
					<-result
				}
				select {
				case <-cancelled:
				case <-time.After(time.Second):
					t.Fatal("decode body remained open")
				}
			})
		}
	}
}

func TestMistralGzipDecode(t *testing.T) {
	for _, status := range []int{200, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == mistralCleanupPath {
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"kv_transfer_params\":"+mistralTestTransfer+"}\n\ndata: [DONE]\n\n")
			}))
			defer prefill.Close()
			decode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Accept-Encoding") != "identity" {
					t.Errorf("encoding=%q", r.Header.Get("Accept-Encoding"))
				}
				var compressed bytes.Buffer
				writer := gzip.NewWriter(&compressed)
				payload := "data: {\"usage\":{\"num_cached_tokens\":3}}\n\ndata: [DONE]\n\n"
				if status == 503 {
					payload = `{"error":"busy"}`
					w.Header().Set("Content-Type", "application/json")
				} else {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				_, _ = io.WriteString(writer, payload)
				_ = writer.Close()
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Content-Length", strconv.Itoa(compressed.Len()))
				w.WriteHeader(status)
				_, _ = w.Write(compressed.Bytes())
			}))
			defer decode.Close()
			server := mistralTestProxy(t, decode, 0)
			request := nativeSafetyRequest(prefill.URL)
			request.Header.Set("Accept-Encoding", "gzip")
			recorder := httptest.NewRecorder()
			server.createRoutes().ServeHTTP(recorder, request)
			if recorder.Code != status || recorder.Header().Get("Content-Encoding") != "" || recorder.Header().Get("Content-Length") != "" {
				t.Errorf("status=%d headers=%v", recorder.Code, recorder.Header())
			}
			if status == 200 && !strings.Contains(recorder.Body.String(), "[DONE]") {
				t.Errorf("body=%s", recorder.Body)
			}
			if status == 503 && recorder.Body.String() != `{"error":"busy"}` {
				t.Errorf("body=%s", recorder.Body)
			}
		})
	}
}

func TestMistralInvalidDescriptorReleasesTransfer(t *testing.T) {
	for _, descriptor := range []string{strings.Replace(mistralTestTransfer, `"remote_port":5557`, `"remote_port":0`, 1), `{"remote_request_id":"transfer-request","remote_port":"invalid"}`} {
		t.Run(descriptor, func(t *testing.T) {
			var released atomic.Int32
			prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == mistralCleanupPath {
					released.Add(1)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"kv_transfer_params\":"+descriptor+"}\n\n")
			}))
			defer prefill.Close()
			server := mistralTestProxy(t, nativeSafetyDecode(t), 0)
			recorder := httptest.NewRecorder()
			server.createRoutes().ServeHTTP(recorder, nativeSafetyRequest(prefill.URL))
			if recorder.Code != 502 || released.Load() != 1 {
				t.Errorf("status=%d cleanup=%d body=%s", recorder.Code, released.Load(), recorder.Body)
			}
		})
	}
}

func TestMistralTimeoutReplacesPrefillError(t *testing.T) {
	prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == mistralCleanupPath {
			time.Sleep(100 * time.Millisecond)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"kv_transfer_params\":"+mistralTestTransfer+",\"error\":{\"code\":503,\"message\":\"busy\"}}\n\n")
	}))
	defer prefill.Close()
	server := mistralTestProxy(t, nativeSafetyDecode(t), 0)
	server.nativeLimits.PrefillTimeout = 50 * time.Millisecond
	recorder := httptest.NewRecorder()
	server.createRoutes().ServeHTTP(recorder, nativeSafetyRequest(prefill.URL))
	if recorder.Code != 504 || !json.Valid(recorder.Body.Bytes()) {
		t.Errorf("status=%d body=%s", recorder.Code, recorder.Body)
	}
}
