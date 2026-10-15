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
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"sync"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
)

const (
	mistralCompletionPart    = "completion_request"
	mistralCachedTokensField = "num_cached_tokens"
)

type mistralPart struct {
	header   textproto.MIMEHeader
	offset   int64
	size     int64
	metadata bool
}

type mistralRequest struct {
	metadata  map[string]any
	boundary  string
	parts     []mistralPart
	spool     *os.File
	spoolOnce sync.Once
}

func (m *mistralRequest) close() {
	m.spoolOnce.Do(func() {
		if m.spool != nil {
			_ = m.spool.Close()
			_ = os.Remove(m.spool.Name())
		}
	})
}

type mistralInputError struct {
	status  int
	message string
}

func (e *mistralInputError) Error() string { return e.message }
func mistralLimitError(message string) error {
	return &mistralInputError{status: http.StatusRequestEntityTooLarge, message: message}
}

type mistralInputReader struct {
	ctx          context.Context
	source       io.Reader
	remaining    int64
	boundary     []byte
	prefix       []byte
	lineLength   int64
	inHeaders    bool
	headerBytes  int
	boundaryLine bool
	boundaryCR   bool
}

func (r *mistralInputReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > r.remaining+1 {
		p = p[:r.remaining+1]
	}
	n, err := r.source.Read(p)
	if int64(n) > r.remaining {
		return 0, mistralLimitError("native request exceeds the configured byte limit")
	}
	r.remaining -= int64(n)
	if len(r.boundary) == 0 {
		return n, err
	}
	// Inspect framing before the MIME parser allocates header maps.
	for index, b := range p[:n] {
		if r.inHeaders {
			r.headerBytes++
			if r.headerBytes > mistralMaxHeaderBytes {
				return index, mistralLimitError("multipart headers exceed the configured byte limit")
			}
		}
		if r.lineLength == 0 {
			r.boundaryLine = true
			r.boundaryCR = false
		}
		r.lineLength++
		if r.lineLength <= int64(len(r.boundary)) {
			r.boundaryLine = r.boundaryLine && b == r.boundary[r.lineLength-1]
		} else {
			switch b {
			case ' ', '\t':
				r.boundaryLine = r.boundaryLine && !r.boundaryCR
			case '\r':
				r.boundaryLine = r.boundaryLine && !r.boundaryCR
				r.boundaryCR = true
			case '\n':
			default:
				r.boundaryLine = false
			}
		}
		if len(r.prefix) < len(r.boundary)+4 {
			r.prefix = append(r.prefix, b)
		}
		if b != '\n' {
			continue
		}
		if r.inHeaders && (r.lineLength == 1 || (r.lineLength == 2 && r.prefix[0] == '\r')) {
			r.inHeaders = false
			r.headerBytes = 0
		} else if !r.inHeaders && r.boundaryLine && r.lineLength > int64(len(r.boundary)) {
			r.inHeaders = true
			r.headerBytes = 0
		}
		r.lineLength = 0
		r.prefix = r.prefix[:0]
	}
	return n, err
}

func readMistralRequest(r *http.Request, apiType reqcommon.APIType, limits mistralLimits) (_ *mistralRequest, err error) {
	defer r.Body.Close()
	m := &mistralRequest{}
	defer func() {
		if err != nil {
			m.close()
		}
	}()
	input := &mistralInputReader{ctx: r.Context(), source: r.Body, remaining: limits.MaxRequestBytes}
	var metadata []byte
	if apiType == reqcommon.APITypeMistralV2 {
		metadata, err = io.ReadAll(io.LimitReader(input, limits.MaxMetadataBytes+1))
		if err != nil {
			return nil, err
		}
		if int64(len(metadata)) > limits.MaxMetadataBytes {
			return nil, mistralLimitError("native JSON exceeds the configured metadata limit")
		}
	} else {
		mediaType, params, parseErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if parseErr != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
			return nil, errors.New("native v3 requires multipart/form-data with a boundary")
		}
		m.boundary = params["boundary"]
		if err := multipart.NewWriter(io.Discard).SetBoundary(m.boundary); err != nil {
			return nil, errors.New("invalid multipart boundary")
		}
		input.boundary = []byte("--" + m.boundary)
		reader := multipart.NewReader(input, m.boundary)
		var offset int64
		for {
			part, nextErr := reader.NextRawPart()
			if nextErr == io.EOF {
				break
			}
			if nextErr != nil {
				return nil, fmt.Errorf("invalid multipart body: %w", nextErr)
			}
			if len(m.parts) >= limits.MaxParts {
				return nil, mistralLimitError("multipart part count exceeds the configured limit")
			}
			record := mistralPart{header: part.Header, offset: offset}
			if part.FormName() == mistralCompletionPart {
				if metadata != nil {
					return nil, errors.New("duplicate completion_request part")
				}
				metadata, err = io.ReadAll(io.LimitReader(part, limits.MaxMetadataBytes+1))
				if err != nil {
					return nil, err
				}
				if int64(len(metadata)) > limits.MaxMetadataBytes {
					return nil, mistralLimitError("completion metadata exceeds the configured byte limit")
				}
				record.metadata = true
			} else {
				if m.spool == nil {
					m.spool, err = os.CreateTemp("", "llm-d-mistral-spool-*")
					if err != nil {
						return nil, &mistralInputError{status: 500, message: "cannot create native request spool"}
					}
				}
				record.size, err = io.Copy(m.spool, part)
				if err != nil {
					return nil, err
				}
				offset += record.size
			}
			m.parts = append(m.parts, record)
		}
		// Include MIME epilogue bytes in the request limit.
		if _, err := io.Copy(io.Discard, input); err != nil {
			return nil, err
		}
		if metadata == nil {
			return nil, errors.New("missing completion_request part")
		}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &fields); err != nil {
		return nil, fmt.Errorf("invalid completion request: %w", err)
	}
	if fields == nil {
		return nil, errors.New("completion request must be a JSON object")
	}
	m.metadata = make(map[string]any, len(fields))
	for key, value := range fields {
		m.metadata[key] = value
	}
	return m, nil
}

type mistralRequestBody struct {
	mu      sync.Mutex
	reader  io.Reader
	closed  bool
	final   bool
	request *mistralRequest
}

func (b *mistralRequestBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, os.ErrClosed
	}
	return b.reader.Read(p)
}
func (b *mistralRequestBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.reader = nil
		if b.final {
			b.request.close()
		}
	}
	return nil
}

func (m *mistralRequest) setBody(r *http.Request, metadata []byte, final ...bool) error {
	if err := r.Context().Err(); err != nil {
		return err
	}
	var readers []io.Reader
	size := int64(len(metadata))
	if m.boundary == "" {
		readers = []io.Reader{bytes.NewReader(metadata)}
	} else {
		var framing bytes.Buffer
		writer := multipart.NewWriter(&framing)
		if err := writer.SetBoundary(m.boundary); err != nil {
			return err
		}
		size = 0
		appendFraming := func() {
			data := bytes.Clone(framing.Bytes())
			readers = append(readers, bytes.NewReader(data))
			size += int64(len(data))
			framing.Reset()
		}
		for _, part := range m.parts {
			headers := textproto.MIMEHeader(http.Header(part.header).Clone())
			if part.metadata {
				headers.Del("Content-Length")
			}
			out, err := writer.CreatePart(headers)
			if err != nil {
				return err
			}
			if part.metadata {
				if _, err := out.Write(metadata); err != nil {
					return err
				}
				appendFraming()
			} else {
				appendFraming()
				readers = append(readers, io.NewSectionReader(m.spool, part.offset, part.size))
				size += part.size
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
		appendFraming()
		r.Header.Set("Content-Type", writer.FormDataContentType())
	}
	r.Body = &mistralRequestBody{reader: io.MultiReader(readers...), request: m, final: len(final) > 0 && final[0]}
	r.ContentLength = size
	r.GetBody = nil
	r.TransferEncoding = nil
	r.Header.Del("Content-Length")
	r.Header.Del("Transfer-Encoding")
	return nil
}

type mistralPrefillError struct {
	status  int
	message string
}

func (e *mistralPrefillError) Error() string { return e.message }

func parseMistralPrefill(body []byte) (map[string]any, error) {
	var transfer mistralTransfer
	if err := readNativePrefill(bytes.NewReader(body), defaultMistralLimits(), &transfer); err != nil {
		return nil, err
	}
	return map[string]any{reqcommon.FieldKVTransferParams: transfer.raw, mistralCachedTokensField: transfer.cachedTokens}, nil
}

func replaceMistralCachedTokensJSON(body []byte, cachedTokens int) ([]byte, bool) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, false
	}
	var usage map[string]json.RawMessage
	if json.Unmarshal(response[reqcommon.FieldUsage], &usage) != nil || usage == nil {
		return body, true
	}
	usage[mistralCachedTokensField], _ = json.Marshal(cachedTokens)
	response[reqcommon.FieldUsage], _ = json.Marshal(usage)
	updated, err := json.Marshal(response)
	if err != nil {
		return body, true
	}
	return updated, true
}
