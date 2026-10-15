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
	"net/url"
	"strings"
	"time"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
)

const mistralCleanupPath = "/v2/free_prefill_blocks"
const mistralCleanupTimeout = 5 * time.Second

type mistralTransfer struct {
	raw          json.RawMessage
	requestID    string
	cachedTokens int
}

func (t *mistralTransfer) consume(event sseEvent) error {
	if !event.hasData {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(event.data, &fields); err != nil || fields == nil {
		return &mistralPrefillError{status: 502, message: "invalid native prefill event"}
	}
	// Capture ownership before processing an error from the same event.
	if raw := fields[reqcommon.FieldKVTransferParams]; len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		var identity struct {
			RequestID string `json:"remote_request_id"`
		}
		if json.Unmarshal(raw, &identity) == nil && identity.RequestID != "" && len(identity.RequestID) <= 1024 {
			// The selected prefill endpoint owns this ID even if transfer fields are invalid.
			t.requestID = identity.RequestID
		}
		var descriptor struct {
			RequestID string          `json:"remote_request_id"`
			EngineID  string          `json:"remote_engine_id"`
			Host      string          `json:"remote_host"`
			Port      int             `json:"remote_port"`
			Blocks    json.RawMessage `json:"remote_block_ids"`
			Prefill   bool            `json:"do_remote_prefill"`
		}
		if err := json.Unmarshal(raw, &descriptor); err != nil || descriptor.RequestID == "" || descriptor.EngineID == "" || descriptor.Host == "" || descriptor.Port < 1 || descriptor.Port > 65535 || !descriptor.Prefill || len(descriptor.RequestID) > 1024 || !bytes.HasPrefix(bytes.TrimSpace(descriptor.Blocks), []byte("[")) {
			return &mistralPrefillError{status: 502, message: "invalid native KV transfer descriptor"}
		}
		t.raw = bytes.Clone(raw)
		t.requestID = descriptor.RequestID
	}
	if raw := fields["error"]; len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		var value struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			return &mistralPrefillError{status: 502, message: "invalid native prefill error"}
		}
		if value.Code < 400 || value.Code > 599 {
			value.Code = 502
		}
		return &mistralPrefillError{status: value.Code, message: value.Message}
	}
	var reason string
	if raw := fields["sequence_finish_reason"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &reason); err != nil {
			return &mistralPrefillError{status: 502, message: "invalid native finish reason"}
		}
	}
	if reason == "abort" || reason == "error" {
		return &mistralPrefillError{status: 503, message: "native prefill " + reason}
	}
	if raw := fields[reqcommon.FieldUsage]; len(raw) > 0 {
		var usage struct {
			Cached *int `json:"num_cached_tokens"`
		}
		if err := json.Unmarshal(raw, &usage); err != nil || (usage.Cached != nil && *usage.Cached < 0) {
			return &mistralPrefillError{status: 502, message: "invalid native cache usage"}
		}
		if usage.Cached != nil {
			t.cachedTokens = *usage.Cached
		}
	}
	return nil
}

func nativeStatus(err error) int {
	var input *mistralInputError
	var prefill *mistralPrefillError
	switch {
	case errors.As(err, &input):
		return input.status
	case errors.As(err, &prefill):
		return prefill.status
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}

func readNativePrefill(reader io.Reader, limits mistralLimits, transfer *mistralTransfer) error {
	parser := newSSEEventParser(limits.MaxEventBytes)
	limited := io.LimitReader(reader, limits.MaxPrefillBytes+1)
	var total int64
	buf := make([]byte, 32<<10)
	done := false
	stop := errors.New("native prefill complete")
	for {
		n, readErr := limited.Read(buf)
		total += int64(n)
		if total > limits.MaxPrefillBytes {
			return &mistralPrefillError{status: 502, message: "native prefill exceeds the response byte limit"}
		}
		err := parser.write(buf[:n], func(event sseEvent) error {
			if event.isDone() {
				done = true
				return stop
			}
			return transfer.consume(event)
		})
		if errors.Is(err, stop) {
			break
		}
		if err != nil {
			return err
		}
		if readErr != nil {
			if readErr != io.EOF {
				return readErr
			}
			break
		}
	}
	if !done || len(transfer.raw) == 0 {
		return &mistralPrefillError{status: 502, message: "native prefill ended without complete transfer metadata and [DONE]"}
	}
	return nil
}

func nativeTarget(prefix, hostPort string) (*url.URL, error) {
	hostPort = strings.TrimPrefix(strings.TrimPrefix(hostPort, "http://"), "https://")
	target, err := url.Parse(prefix + hostPort)
	if err != nil || target.Hostname() == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || (target.Path != "" && target.Path != "/") {
		return nil, errors.New("invalid native engine endpoint")
	}
	return target, nil
}

func prepareNativeHTTP(r *http.Request, target *url.URL) *http.Request {
	out := r.Clone(r.Context())
	u := *r.URL
	u.Scheme = target.Scheme
	u.Host = target.Host
	out.URL = &u
	out.RequestURI = ""
	out.Host = target.Host
	for _, field := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		out.Header.Del(field)
	}
	out.Header.Set("Accept-Encoding", "identity")
	return out
}

func nativeResponseBody(response *http.Response) (io.Reader, error) {
	switch response.Header.Get("Content-Encoding") {
	case "", "identity":
		return response.Body, nil
	case "gzip":
		return gzip.NewReader(response.Body)
	default:
		return nil, errors.New("unsupported native response encoding")
	}
}

func (s *Server) cleanupMistralTransfer(ctx context.Context, target *url.URL, transfer mistralTransfer, headers http.Header) {
	if transfer.requestID == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mistralCleanupTimeout)
	defer cancel()
	endpoint := *target
	endpoint.Path = mistralCleanupPath
	endpoint.RawPath = ""
	endpoint.RawQuery = ""
	payload, _ := json.Marshal(map[string]any{"request_ids": []string{transfer.requestID}})
	request, err := http.NewRequestWithContext(cleanupCtx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return
	}
	request.Header.Set("Content-Type", reqcommon.ContentTypeJSON)
	request.Header.Set("Authorization", headers.Get("Authorization"))
	response, err := s.nativePrefillClient.Do(request)
	if err != nil {
		s.logger.Error(err, "native KV cleanup failed")
		return
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode >= 400 {
		s.logger.Error(fmt.Errorf("cleanup HTTP status %d", response.StatusCode), "native KV cleanup rejected")
	}
}

func (s *Server) prefillMistral(request *http.Request, target *url.URL, envelope *mistralRequest, payload []byte, transfer *mistralTransfer) *bufferedResponseWriter {
	reply := &bufferedResponseWriter{}
	request = prepareNativeHTTP(request, target)
	request.Header.Del(requestHeaderDataParallelRank)
	if err := envelope.setBody(request, payload); err != nil {
		_ = sendError(err, "NativeRequestError", 500, reply)
		return reply
	}
	defer request.Body.Close()
	response, err := s.nativePrefillClient.Do(request)
	if response != nil {
		defer response.Body.Close()
		reply.headers = response.Header.Clone()
		reply.headers.Del("Content-Length")
		reply.headers.Del("Content-Encoding")
		reader, bodyErr := nativeResponseBody(response)
		switch {
		case bodyErr != nil:
			err = bodyErr
		case response.StatusCode >= 400:
			reply.statusCode = response.StatusCode
			_, err = io.Copy(&reply.buffer, io.LimitReader(reader, int64(s.nativeLimits.MaxEventBytes)+1))
			if reply.buffer.Len() > s.nativeLimits.MaxEventBytes {
				err = errors.New("native error response exceeds its byte limit")
			}
			if err == nil {
				return reply
			}
		default:
			err = readNativePrefill(reader, s.nativeLimits, transfer)
		}
	}
	if err != nil {
		reply.buffer.Reset()
		_ = sendError(err, "NativePrefillError", nativeStatus(err), reply)
		return reply
	}
	reply.statusCode = 200
	return reply
}
