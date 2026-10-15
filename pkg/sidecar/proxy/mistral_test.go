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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/common/routing"
	"github.com/llm-d/llm-d-router/pkg/sidecar/constants"
)

const mistralTestRequest = `{"request_id":"native-id","model":"mistral-small-2603","prompt":[1,2,3],"max_tokens":128,"min_tokens":32,"cache_salt":"tenant","seed":9007199254740993}`
const mistralTestTransfer = `{"do_remote_prefill":true,"remote_host":"127.0.0.1","remote_engine_id":"engine","remote_request_id":"transfer-request","remote_port":5557,"remote_block_ids":[[9007199254740993]],"remote_dp_rank":0}`
const mistralImagePayload = "\x00\xff\r\n--boundary-like\x00xxxx"

func mistralTestBody(t *testing.T, version int, metadata string) ([]byte, string) {
	t.Helper()
	if version == 2 {
		return []byte(metadata), "application/json"
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(metadata), &fields) == nil && fields != nil {
		fields["images"] = json.RawMessage(`[{"index":0,"shape":[2,4,3],"dtype":"uint8"}]`)
		encoded, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		metadata = string(encoded)
	}
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormField("completion_request")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(part, metadata); err != nil {
		t.Fatal(err)
	}
	part, err = w.CreatePart(textproto.MIMEHeader{
		"Content-Disposition": {`form-data; name="image_0"; filename="image_0.bin"`},
		"Content-Type":        {"application/octet-stream"},
		"X-Part-Metadata":     {"keep-me"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte(mistralImagePayload)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), w.FormDataContentType()
}

func mistralReadTestBody(t *testing.T, r *http.Request, version int) map[string]json.RawMessage {
	t.Helper()
	var raw []byte
	var err error
	if version == 2 {
		raw, err = io.ReadAll(r.Body)
	} else {
		reader, readErr := r.MultipartReader()
		if readErr != nil {
			t.Error(readErr)
			return nil
		}
		for {
			part, nextErr := reader.NextPart()
			if nextErr == io.EOF {
				break
			}
			if nextErr != nil {
				t.Error(nextErr)
				return nil
			}
			data, readErr := io.ReadAll(part)
			if readErr != nil {
				t.Error(readErr)
				return nil
			}
			if part.FormName() == "completion_request" {
				raw = data
			} else if !bytes.Equal(data, []byte(mistralImagePayload)) || part.Header.Get("X-Part-Metadata") != "keep-me" {
				t.Errorf("binary part changed: %q, %v", data, part.Header)
			}
		}
	}
	if err != nil {
		t.Error(err)
		return nil
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Error(err)
	}
	if version == 3 {
		var images []struct {
			Index int    `json:"index"`
			Shape []int  `json:"shape"`
			DType string `json:"dtype"`
		}
		if err := json.Unmarshal(body["images"], &images); err != nil || len(images) != 1 || images[0].Index != 0 || len(images[0].Shape) != 3 || images[0].Shape[0]*images[0].Shape[1]*images[0].Shape[2] != len(mistralImagePayload) || images[0].DType != "uint8" {
			t.Errorf("invalid array metadata: %s", body["images"])
		}
	}
	return body
}

func mistralTestProxy(t *testing.T, decode *httptest.Server, retries int) *Server {
	t.Helper()
	endpoint, err := url.Parse(decode.URL)
	if err != nil {
		t.Fatal(err)
	}
	s := NewProxy(Config{DecoderURL: endpoint, KVConnector: constants.KVConnectorNIXLV2, PrefillMaxRetries: retries})
	s.allowlistValidator = &AllowlistValidator{}
	s.forwardDataParallel = false
	s.setKVConnector()
	return s
}

func TestMistralRemotePrefill(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("TMPDIR", tmp)
			var prefillCalls, decodeCalls atomic.Int32
			prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := mistralReadTestBody(t, r, version)
				if string(body[reqcommon.FieldMaxTokens]) != "1" || body[reqcommon.FieldMinTokens] != nil {
					t.Error("prefill token limits are incorrect")
				}
				if body[reqcommon.FieldStream] != nil || body[reqcommon.FieldStreamOptions] != nil {
					t.Error("native prefill has OpenAI streaming fields")
				}
				var params map[string]any
				if err := json.Unmarshal(body[reqcommon.FieldKVTransferParams], &params); err != nil || params[reqcommon.FieldDoRemoteDecode] != true {
					t.Errorf("prefill transfer: %v, %v", params, err)
				}
				if prefillCalls.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				stream := "data: {\"index\":0,\"token_ids\":[1],\"usage\":{}}\n\n" +
					"data: {\"sequence_finish_reason\":\"length\",\"usage\":{\"num_cached_tokens\":2},\"kv_transfer_params\":" + mistralTestTransfer + "}\n\ndata: [DONE]\n\n"
				for _, chunk := range strings.SplitAfter(stream, "data") {
					_, _ = io.WriteString(w, chunk)
					w.(http.Flusher).Flush()
				}
			}))
			defer prefill.Close()
			decode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				decodeCalls.Add(1)
				body := mistralReadTestBody(t, r, version)
				if string(body[reqcommon.FieldMaxTokens]) != "128" || string(body[reqcommon.FieldMinTokens]) != "32" || string(body["seed"]) != "9007199254740993" {
					t.Errorf("decode sampling changed: %s", body)
				}
				if !bytes.Contains(body[reqcommon.FieldKVTransferParams], []byte("9007199254740993")) {
					t.Error("transfer integer lost precision")
				}
				if r.Header.Get(requestHeaderDataParallelRank) != "7" {
					t.Error("decode rank changed")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"token_ids\":[42],\"usage\":{\"num_cached_tokens\":3}}\n\ndata: [DONE]\n\n")
			}))
			defer decode.Close()
			s := mistralTestProxy(t, decode, 1)
			body, contentType := mistralTestBody(t, version, mistralTestRequest)
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v%d/completions", version), bytes.NewReader(body))
			req.Header.Set("Content-Type", contentType)
			req.Header.Set(routing.PrefillEndpointHeader, strings.TrimPrefix(prefill.URL, "http://"))
			req.Header.Set(requestHeaderDataParallelRank, "7")
			recorder := httptest.NewRecorder()
			s.createRoutes().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusOK || prefillCalls.Load() != 2 || decodeCalls.Load() != 1 {
				t.Fatalf("status=%d prefill=%d decode=%d body=%s", recorder.Code, prefillCalls.Load(), decodeCalls.Load(), recorder.Body)
			}
			if !strings.Contains(recorder.Body.String(), `"num_cached_tokens":2`) || strings.Contains(recorder.Body.String(), "prompt_tokens_details") || !strings.HasSuffix(recorder.Body.String(), "data: [DONE]\n\n") {
				t.Errorf("native response changed: %s", recorder.Body)
			}
			files, err := os.ReadDir(tmp)
			if err != nil || len(files) != 0 {
				t.Errorf("temporary files remain: %v, %v", files, err)
			}
		})
	}
}

func TestMistralDecoderOnly(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			body, contentType := mistralTestBody(t, version, mistralTestRequest)
			decode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, _ := io.ReadAll(r.Body)
				if !bytes.Equal(got, body) || r.Header.Get("Content-Type") != contentType {
					t.Error("decoder-only request changed")
				}
				_, _ = io.WriteString(w, "data: [DONE]\n\n")
			}))
			defer decode.Close()
			s := mistralTestProxy(t, decode, 0)
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v%d/completions", version), bytes.NewReader(body))
			req.Header.Set("Content-Type", contentType)
			recorder := httptest.NewRecorder()
			s.createRoutes().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusOK {
				t.Errorf("status=%d body=%s", recorder.Code, recorder.Body)
			}
		})
	}
}

func TestMistralPrefillFailure(t *testing.T) {
	cases := []struct {
		name, stream string
		status       int
	}{
		{"missing metadata", "data: {}\n\ndata: [DONE]\n\n", 502},
		{"truncated", "data: {\"kv_transfer_params\":" + mistralTestTransfer + "}\n\n", 502},
		{"invalid JSON", "data: broken\n\ndata: [DONE]\n\n", 502},
		{"abort", "data: {\"sequence_finish_reason\":\"abort\"}\n\n", 503},
		{"error", "data: {\"sequence_finish_reason\":\"error\"}\n\n", 503},
		{"stream error", "data: {\"error\":{\"code\":429,\"message\":\"busy\"}}\n\n", 429},
		{"invalid transfer", "data: {\"kv_transfer_params\":{}}\n\ndata: [DONE]\n\n", 502},
		{"null event", "data: null\n\ndata: [DONE]\n\n", 502},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("TMPDIR", tmp)
			var decoded atomic.Bool
			prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tt.stream)
			}))
			defer prefill.Close()
			decode := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { decoded.Store(true) }))
			defer decode.Close()
			s := mistralTestProxy(t, decode, 0)
			body, contentType := mistralTestBody(t, 3, mistralTestRequest)
			req := httptest.NewRequest(http.MethodPost, "/v3/completions", bytes.NewReader(body))
			req.Header.Set("Content-Type", contentType)
			req.Header.Set(routing.PrefillEndpointHeader, strings.TrimPrefix(prefill.URL, "http://"))
			recorder := httptest.NewRecorder()
			s.createRoutes().ServeHTTP(recorder, req)
			if recorder.Code != tt.status || decoded.Load() {
				t.Errorf("status=%d decode=%v body=%s", recorder.Code, decoded.Load(), recorder.Body)
			}
			files, err := os.ReadDir(tmp)
			if err != nil || len(files) != 0 {
				t.Errorf("temporary files remain after prefill failure: %v, %v", files, err)
			}
		})
	}
}

func TestMistralInvalidRequest(t *testing.T) {
	valid, contentType := mistralTestBody(t, 3, mistralTestRequest)
	invalid, invalidContentType := mistralTestBody(t, 3, "{")
	cases := []struct {
		name, path, contentType string
		body                    []byte
	}{
		{"invalid JSON", "/v2/completions", "application/json", []byte("{")},
		{"invalid v3 metadata", "/v3/completions", invalidContentType, invalid},
		{"null JSON", "/v2/completions", "application/json", []byte("null")},
		{"missing boundary", "/v3/completions", "multipart/form-data", valid},
		{"truncated multipart", "/v3/completions", contentType, valid[:len(valid)-20]},
		{"missing metadata", "/v3/completions", "multipart/form-data; boundary=test", []byte("--test\r\nContent-Disposition: form-data; name=other\r\n\r\nvalue\r\n--test--\r\n")},
		{"duplicate metadata", "/v3/completions", "multipart/form-data; boundary=test", []byte("--test\r\nContent-Disposition: form-data; name=completion_request\r\n\r\n{}\r\n--test\r\nContent-Disposition: form-data; name=completion_request\r\n\r\n{}\r\n--test--\r\n")},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("TMPDIR", tmp)
			var called atomic.Bool
			upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { called.Store(true) }))
			defer upstream.Close()
			s := mistralTestProxy(t, upstream, 0)
			req := httptest.NewRequest(http.MethodPost, tt.path, bytes.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			req.Header.Set(routing.PrefillEndpointHeader, strings.TrimPrefix(upstream.URL, "http://"))
			recorder := httptest.NewRecorder()
			s.createRoutes().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest || called.Load() {
				t.Errorf("status=%d upstream=%v body=%s", recorder.Code, called.Load(), recorder.Body)
			}
			files, err := os.ReadDir(tmp)
			if err != nil || len(files) != 0 {
				t.Errorf("temporary files remain: %v, %v", files, err)
			}
		})
	}
}

func TestMistralUnsupportedModes(t *testing.T) {
	for _, mode := range []string{"write", "parallel", "sglang"} {
		t.Run(mode, func(t *testing.T) {
			var called atomic.Bool
			upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { called.Store(true) }))
			defer upstream.Close()
			s := mistralTestProxy(t, upstream, 0)
			s.config.MoRIIOWriteMode = mode == "write"
			s.config.MoRIIOParallelDispatch = mode == "parallel"
			if mode == "sglang" {
				s.config.KVConnector = constants.KVConnectorSGLang
				s.setKVConnector()
			}
			req := httptest.NewRequest(http.MethodPost, "/v2/completions", strings.NewReader(mistralTestRequest))
			req.Header.Set(routing.PrefillEndpointHeader, strings.TrimPrefix(upstream.URL, "http://"))
			recorder := httptest.NewRecorder()
			s.createRoutes().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest || called.Load() {
				t.Errorf("status=%d upstream=%v", recorder.Code, called.Load())
			}
		})
	}
}

func TestMistralCancellation(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	prefill := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer prefill.Close()
	var decoded atomic.Bool
	decode := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { decoded.Store(true) }))
	defer decode.Close()
	s := mistralTestProxy(t, decode, 0)
	body, contentType := mistralTestBody(t, 3, mistralTestRequest)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v3/completions", bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set(routing.PrefillEndpointHeader, strings.TrimPrefix(prefill.URL, "http://"))
	finished := make(chan struct{})
	handler := s.createRoutes()
	go func() { defer close(finished); handler.ServeHTTP(httptest.NewRecorder(), req) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("prefill did not start")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("sidecar did not stop")
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("prefill was not cancelled")
	}
	if decoded.Load() {
		t.Error("decode started after cancellation")
	}
	files, err := os.ReadDir(tmp)
	if err != nil || len(files) != 0 {
		t.Errorf("temporary files remain after cancellation: %v, %v", files, err)
	}
}

func TestMistralLargePrefillEvent(t *testing.T) {
	stream := ": comment\r\n\r\ndata: {\"padding\":\"" + strings.Repeat("x", 128*1024) + "\",\r\ndata: \"kv_transfer_params\":" + mistralTestTransfer + "}\r\n\r\ndata: [DONE]\r\n\r\n"
	response, err := parseMistralPrefill([]byte(stream))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(response[reqcommon.FieldKVTransferParams].(json.RawMessage), []byte("9007199254740993")) {
		t.Error("transfer metadata changed")
	}
}

func TestMistralProtocolFixtures(t *testing.T) {
	request, err := os.ReadFile("testdata/mistral/request.json")
	if err != nil {
		t.Fatal(err)
	}
	prefillStream, err := os.ReadFile("testdata/mistral/prefill.sse")
	if err != nil {
		t.Fatal(err)
	}
	decodeStream, err := os.ReadFile("testdata/mistral/decode.sse")
	if err != nil {
		t.Fatal(err)
	}
	for _, apiType := range []reqcommon.APIType{reqcommon.APITypeMistralV2, reqcommon.APITypeMistralV3} {
		t.Run(apiType.String(), func(t *testing.T) {
			version := 2
			if apiType == reqcommon.APITypeMistralV3 {
				version = 3
			}
			var prefilled, decoded atomic.Bool
			prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				prefilled.Store(true)
				body := mistralReadTestBody(t, r, version)
				if string(body[reqcommon.FieldMaxTokens]) != "1" || string(body["prompt"]) != "[0,1,2]" {
					t.Errorf("invalid fixture prefill: %s", body)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write(prefillStream)
			}))
			defer prefill.Close()
			decode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				decoded.Store(true)
				body := mistralReadTestBody(t, r, version)
				if string(body[reqcommon.FieldMaxTokens]) != "20" || !bytes.Contains(body[reqcommon.FieldKVTransferParams], []byte(`"remote_block_ids":[109]`)) {
					t.Errorf("invalid fixture decode: %s", body)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write(decodeStream)
			}))
			defer decode.Close()
			s := mistralTestProxy(t, decode, 0)
			body, contentType := mistralTestBody(t, version, string(request))
			req := httptest.NewRequest(http.MethodPost, apiType.Path(), bytes.NewReader(body))
			req.Header.Set("Content-Type", contentType)
			req.Header.Set(routing.PrefillEndpointHeader, strings.TrimPrefix(prefill.URL, "http://"))
			recorder := httptest.NewRecorder()
			s.createRoutes().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusOK || !prefilled.Load() || !decoded.Load() || !strings.Contains(recorder.Body.String(), `"token_ids":[3]`) {
				t.Errorf("fixture failed: status=%d, body=%s", recorder.Code, recorder.Body)
			}
		})
	}
}
