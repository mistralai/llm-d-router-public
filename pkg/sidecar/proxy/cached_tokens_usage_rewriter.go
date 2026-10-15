/*
Copyright 2025 The llm-d Authors.

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
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/felixge/httpsnoop"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
)

// OpenAI-compatible chat usage reports prompt cache hits at
// usage.prompt_tokens_details.cached_tokens.
// See: https://platform.openai.com/docs/guides/prompt-caching
type cachedTokensUsageRewriter struct {
	header       http.Header
	cachedTokens int
	wroteHeader  bool
	streaming    bool
	mistral      bool
	httpError    bool
	nativeParser *sseEventParser
	nativeState  *mistralDecodeState
	streamBuffer []byte
	jsonBuffer   []byte
}

// usageKey is the JSON key that must be present before a frame can carry usage.
// Streamed responses send one frame per token and only the final frame has usage,
// so scanning for this is much cheaper than unmarshalling every frame to find out.
// The openai and anthropic stream parsers scan for the same word without the
// quotes; a JSON serializer always writes the key quoted, so keeping them here
// skips more content frames.
var usageKey = []byte(`"` + reqcommon.FieldUsage + `"`)

func newCachedTokensResponseWriter(
	w http.ResponseWriter, cachedTokens int, streaming bool,
) http.ResponseWriter {
	writer, _ := newCachedTokensResponseWriterWithFinalize(w, cachedTokens, streaming)
	return writer
}

func newCachedTokensResponseWriterWithFinalize(
	w http.ResponseWriter, cachedTokens int, streaming bool,
) (http.ResponseWriter, func() error) {
	return newCachedTokensResponseWriterForAPI(w, cachedTokens, streaming, reqcommon.APITypeChatCompletions)
}

type mistralDecodeState struct {
	done     bool
	terminal bool
	failed   bool
}

func newCachedTokensResponseWriterForAPI(w http.ResponseWriter, cachedTokens int, streaming bool, apiType reqcommon.APIType) (http.ResponseWriter, func() error) {
	return newCachedTokensResponseWriterForAPIWithLimits(w, cachedTokens, streaming, apiType, defaultMistralLimits(), &mistralDecodeState{})
}

func newCachedTokensResponseWriterForAPIWithLimits(w http.ResponseWriter, cachedTokens int, streaming bool, apiType reqcommon.APIType, limits mistralLimits, state *mistralDecodeState) (http.ResponseWriter, func() error) {
	rewriter := &cachedTokensUsageRewriter{
		mistral:      apiType.IsMistral(),
		nativeParser: newSSEEventParser(limits.MaxEventBytes),
		nativeState:  state,
		header:       w.Header(),
		cachedTokens: cachedTokens,
		streaming:    streaming,
	}
	// httpsnoop preserves optional ResponseWriter interfaces such as
	// http.Flusher, http.Hijacker, http.Pusher, and io.ReaderFrom.
	writer := httpsnoop.Wrap(w, httpsnoop.Hooks{
		WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(statusCode int) {
				rewriter.writeHeader(next, statusCode)
			}
		},
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(body []byte) (int, error) {
				return rewriter.write(next, body)
			}
		},
		ReadFrom: func(_ httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				// ReadFrom would otherwise bypass Write and skip usage rewriting.
				return rewriter.readFrom(w.Write, src)
			}
		},
	})
	return writer, func() error {
		if rewriter.mistral && rewriter.httpError {
			return nil
		}
		if rewriter.mistral && rewriter.streaming {
			if err := rewriter.nativeParser.finish(); err != nil {
				return err
			}
			if !rewriter.nativeState.terminal {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
		if rewriter.streaming {
			return rewriter.flushSSEBuffer(w.Write)
		}
		return rewriter.flushJSONBuffer(w.Write)
	}
}

func (r *cachedTokensUsageRewriter) writeHeader(next httpsnoop.WriteHeaderFunc, statusCode int) {
	// Rewriting may change the body size, so any upstream Content-Length is stale.
	r.header.Del("Content-Length")
	r.wroteHeader = true
	if statusCode >= 400 {
		r.httpError = true
		if r.nativeState != nil {
			r.nativeState.failed = true
		}
	}
	next(statusCode)
}

func (r *cachedTokensUsageRewriter) write(next httpsnoop.WriteFunc, body []byte) (int, error) {
	if r.mistral && r.httpError {
		return next(body)
	}
	if r.mistral && r.streaming {
		return r.writeNative(next, body)
	}
	if !r.streaming {
		return r.writeJSON(next, body)
	}
	updated := r.rewriteSSEChunk(body)
	if !r.wroteHeader {
		r.header.Del("Content-Length")
	}
	n, err := next(updated)
	if err != nil {
		return n, err
	}
	return len(body), nil
}

func (r *cachedTokensUsageRewriter) writeJSON(next httpsnoop.WriteFunc, body []byte) (int, error) {
	// ReverseProxy may split one JSON document across multiple Write calls.
	r.jsonBuffer = append(r.jsonBuffer, body...)
	updated, ok := replaceCachedTokensJSON(r.jsonBuffer, r.cachedTokens)
	if !ok {
		return len(body), nil
	}
	r.jsonBuffer = nil
	if !r.wroteHeader {
		r.header.Del("Content-Length")
	}
	n, err := next(updated)
	if err != nil {
		return n, err
	}
	return len(body), nil
}

func (r *cachedTokensUsageRewriter) readFrom(next httpsnoop.WriteFunc, src io.Reader) (int64, error) {
	if r.mistral && r.httpError {
		return io.Copy(cachedTokensStreamWriter{write: r.write, forward: next}, src)
	}
	if r.streaming {
		// SSE can be long-lived, so keep it streaming and rewrite complete lines.
		n, err := io.Copy(cachedTokensStreamWriter{
			write:   r.write,
			forward: next,
		}, src)
		if err != nil {
			return n, err
		}
		if r.mistral {
			return n, r.nativeParser.finish()
		}
		if err := r.flushSSEBuffer(next); err != nil {
			return n, err
		}
		return n, nil
	}

	// Non-streaming JSON must be complete before we can safely rewrite usage.
	body, err := io.ReadAll(src)
	if err != nil {
		return 0, err
	}
	_, err = r.write(next, body)
	if err != nil {
		return 0, err
	}
	return int64(len(body)), nil
}

type cachedTokensStreamWriter struct {
	write   func(httpsnoop.WriteFunc, []byte) (int, error)
	forward httpsnoop.WriteFunc
}

func (w cachedTokensStreamWriter) Write(body []byte) (int, error) {
	return w.write(w.forward, body)
}

func (r *cachedTokensUsageRewriter) rewriteSSEChunk(body []byte) []byte {
	r.streamBuffer = append(r.streamBuffer, body...)
	updated := make([]byte, 0, len(r.streamBuffer))
	for {
		lineEnd := bytes.IndexByte(r.streamBuffer, '\n')
		if lineEnd < 0 {
			break
		}
		// Upstream may split chunks mid-event; only rewrite complete SSE lines.
		line := r.streamBuffer[:lineEnd+1]
		replacedLine, _ := r.replaceSSELine(line)
		updated = append(updated, replacedLine...)
		r.streamBuffer = r.streamBuffer[lineEnd+1:]
	}
	return updated
}

func (r *cachedTokensUsageRewriter) flushSSEBuffer(next httpsnoop.WriteFunc) error {
	if len(r.streamBuffer) == 0 {
		return nil
	}
	// EOF without a final newline is malformed SSE, but forward it rather than
	// dropping bytes. If it is a data line, still try to apply the usage rewrite.
	replacedLine, _ := r.replaceSSELine(r.streamBuffer)
	r.streamBuffer = nil
	_, err := next(replacedLine)
	return err
}

func (r *cachedTokensUsageRewriter) flushJSONBuffer(next httpsnoop.WriteFunc) error {
	if len(r.jsonBuffer) == 0 {
		return nil
	}
	updated := replaceCachedTokens(r.jsonBuffer, r.cachedTokens)
	r.jsonBuffer = nil
	if !r.wroteHeader {
		r.header.Del("Content-Length")
	}
	_, err := next(updated)
	return err
}

func extractCachedTokens(response map[string]any) (int, bool) {
	usage, ok := response[reqcommon.FieldUsage].(map[string]any)
	if !ok {
		return 0, false
	}
	return cachedTokensFromUsage(usage)
}

func cachedTokensFromUsage(usage map[string]any) (int, bool) {
	// Only the documented OpenAI-compatible field is used as the source of truth.
	details, ok := usage[reqcommon.FieldPromptTokensDetails].(map[string]any)
	if !ok {
		return 0, false
	}
	if cachedTokens, ok := intValue(details[reqcommon.FieldCachedTokens]); ok {
		return cachedTokens, true
	}
	return 0, false
}

func intValue(value any) (int, bool) {
	switch v := value.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	default:
		return 0, false
	}
}

func replaceCachedTokens(body []byte, cachedTokens int) []byte {
	if len(bytes.TrimSpace(body)) == 0 {
		return body
	}
	// Prefer full JSON first; streamed responses are handled line-by-line below.
	if updated, ok := replaceCachedTokensJSON(body, cachedTokens); ok {
		return updated
	}
	if updated, ok := replaceCachedTokensSSE(body, cachedTokens); ok {
		return updated
	}
	return body
}

func replaceCachedTokensJSON(body []byte, cachedTokens int) ([]byte, bool) {
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, false
	}
	if !setCachedTokens(response, cachedTokens) {
		return body, true
	}
	updated, err := json.Marshal(response)
	if err != nil {
		return body, true
	}
	return updated, true
}

func replaceCachedTokensSSE(body []byte, cachedTokens int) ([]byte, bool) {
	lines := bytes.SplitAfter(body, []byte("\n"))
	updated := make([]byte, 0, len(body))
	changed := false
	processed := false

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		replacedLine, ok := replaceCachedTokensSSELine(line, cachedTokens)
		if !ok {
			updated = append(updated, replacedLine...)
			continue
		}
		processed = true
		if !bytes.Equal(replacedLine, line) {
			changed = true
		}
		updated = append(updated, replacedLine...)
	}

	if !processed {
		return nil, false
	}
	if !changed {
		return body, true
	}
	return updated, true
}

func (r *cachedTokensUsageRewriter) replaceSSELine(line []byte) ([]byte, bool) {
	if r.mistral {
		return replaceCachedTokensSSELineWithRewriter(line, r.cachedTokens, replaceMistralCachedTokensJSON)
	}
	return replaceCachedTokensSSELine(line, r.cachedTokens)
}

func replaceCachedTokensSSELine(line []byte, cachedTokens int) ([]byte, bool) {
	return replaceCachedTokensSSELineWithRewriter(line, cachedTokens, replaceCachedTokensJSON)
}

func replaceCachedTokensSSELineWithRewriter(line []byte, cachedTokens int, rewrite func([]byte, int) ([]byte, bool)) ([]byte, bool) {
	trimmedLine := bytes.TrimRight(line, "\r\n")
	lineEnding := line[len(trimmedLine):]
	data, ok := bytes.CutPrefix(trimmedLine, []byte(reqcommon.SSEDataPrefix))
	if !ok {
		return line, false
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte(reqcommon.SSEDoneMarker)) {
		return line, true
	}
	if !bytes.Contains(data, usageKey) {
		// No usage in this frame, so unmarshalling it could not change anything.
		return line, true
	}
	// Only JSON data frames can carry usage; other SSE frames pass through.
	replacedData, isJSON := rewrite(data, cachedTokens)
	if !isJSON {
		return line, true
	}
	updated := make([]byte, 0, len(line))
	updated = append(updated, []byte(reqcommon.SSEDataPrefix)...)
	updated = append(updated, replacedData...)
	updated = append(updated, lineEnding...)
	return updated, true
}

func setCachedTokens(response map[string]any, cachedTokens int) bool {
	usage, ok := response[reqcommon.FieldUsage].(map[string]any)
	if !ok {
		return false
	}
	changed := false
	details, ok := usage[reqcommon.FieldPromptTokensDetails].(map[string]any)
	if !ok {
		// Some decoder chunks omit details entirely; create the standard field.
		usage[reqcommon.FieldPromptTokensDetails] = map[string]any{reqcommon.FieldCachedTokens: cachedTokens}
		return true
	}
	if current, ok := intValue(details[reqcommon.FieldCachedTokens]); !ok || current != cachedTokens {
		details[reqcommon.FieldCachedTokens] = cachedTokens
		changed = true
	}
	return changed
}

func (r *cachedTokensUsageRewriter) writeNative(next httpsnoop.WriteFunc, body []byte) (int, error) {
	stop := errors.New("native SSE terminal event")
	err := r.nativeParser.write(body, func(event sseEvent) error {
		if r.nativeState.terminal {
			return stop
		}
		out := event.raw
		if event.isDone() {
			r.nativeState.done = true
			r.nativeState.terminal = true
		} else if event.hasData {
			var fields struct {
				Reason string          `json:"sequence_finish_reason"`
				Error  json.RawMessage `json:"error"`
			}
			if err := json.Unmarshal(event.data, &fields); err != nil || bytes.Equal(bytes.TrimSpace(event.data), []byte("null")) {
				return errors.New("invalid native decode event")
			}
			if fields.Reason == "abort" || fields.Reason == "error" || (len(fields.Error) > 0 && !bytes.Equal(bytes.TrimSpace(fields.Error), []byte("null"))) {
				r.nativeState.failed = true
				r.nativeState.terminal = true
			}
			if updated, ok := replaceMistralCachedTokensJSON(event.data, r.cachedTokens); ok && !bytes.Equal(updated, event.data) {
				out = event.rewrite(updated)
			}
		}
		written, err := next(out)
		if err != nil {
			return err
		}
		if written != len(out) {
			return io.ErrShortWrite
		}
		if r.nativeState.terminal {
			return stop
		}
		return nil
	})
	if errors.Is(err, stop) {
		err = nil
	}
	if err != nil {
		return 0, err
	}
	return len(body), nil
}
