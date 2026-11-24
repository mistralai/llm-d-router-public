/*
Copyright 2026 The Kubernetes Authors.

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

package parserutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"unicode/utf8"

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
)

var ErrTrailingData = errors.New("unexpected trailing data after JSON value")

// ParseRenderRequest reads only the envelope required for model routing.
func ParseRenderRequest(data []byte) (*fwkrh.ParseResult, error) {
	payload, err := UnmarshalEnvelope(data, "prompt", "system")
	if err != nil {
		return nil, err
	}
	model, _ := payload["model"].(string)
	return &fwkrh.ParseResult{
		Body:                   &fwkrh.InferenceRequestBody{Payload: fwkrh.PayloadMap(payload), RawBody: data, Model: model, RenderRequest: true},
		SkipResponseProcessing: true,
	}, nil
}

// UnmarshalEnvelope keeps objects, arrays, and rawFields opaque so routing
// metadata edits cannot round-trip content through Go values.
func UnmarshalEnvelope(data []byte, rawFields ...string) (map[string]any, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		if fallbackErr := Unmarshal(data, &fields); fallbackErr != nil {
			return nil, fallbackErr
		}
	}
	return envelopeFromFields(fields, rawFields)
}

// UnmarshalValidatedEnvelope returns the UnmarshalEnvelope result without
// decoding data again. The caller must have validated the complete, unchanged
// data as JSON, for example with a successful json.Unmarshal.
func UnmarshalValidatedEnvelope(data []byte, rawFields ...string) (map[string]any, error) {
	fields, ok := splitValidatedObject(data)
	if !ok {
		return UnmarshalEnvelope(data, rawFields...)
	}
	return envelopeFromFields(fields, rawFields)
}

func envelopeFromFields(fields map[string]json.RawMessage, rawFields []string) (map[string]any, error) {
	result := make(map[string]any, len(fields))
	for key, raw := range fields {
		if slices.Contains(rawFields, key) || raw[0] == '{' || raw[0] == '[' {
			result[key] = raw
			continue
		}
		var value any
		decode := Unmarshal
		if raw[0] == '"' {
			decode = json.Unmarshal
		}
		if err := decode(raw, &value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

// splitValidatedObject returns copies of the member values of a validated JSON
// object, matching a decode into map[string]json.RawMessage. It reports false
// when data is not an object or a member boundary is missing.
func splitValidatedObject(data []byte) (map[string]json.RawMessage, bool) {
	i := skipJSONSpace(data, 0)
	if i == len(data) || data[i] != '{' {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	i = skipJSONSpace(data, i+1)
	if i < len(data) && data[i] == '}' {
		return fields, true
	}
	for i < len(data) && data[i] == '"' {
		keyEnd := validatedStringEnd(data, i)
		if keyEnd < 0 {
			return nil, false
		}
		key, ok := validatedObjectKey(data[i:keyEnd])
		i = skipJSONSpace(data, keyEnd)
		if !ok || i == len(data) || data[i] != ':' {
			return nil, false
		}
		i = skipJSONSpace(data, i+1)
		valueEnd := validatedValueEnd(data, i)
		if valueEnd < 0 {
			return nil, false
		}
		fields[key] = bytes.Clone(data[i:valueEnd])
		i = skipJSONSpace(data, valueEnd)
		if i == len(data) {
			return nil, false
		}
		switch data[i] {
		case '}':
			return fields, true
		case ',':
			i = skipJSONSpace(data, i+1)
		default:
			return nil, false
		}
	}
	return nil, false
}

// validatedObjectKey decodes a quoted key. Keys with escapes or non-ASCII bytes
// go through json.Unmarshal so unescaping and invalid UTF-8 replacement match
// map key decoding.
func validatedObjectKey(quoted []byte) (string, bool) {
	inner := quoted[1 : len(quoted)-1]
	for _, c := range inner {
		if c == '\\' || c >= utf8.RuneSelf {
			var key string
			err := json.Unmarshal(quoted, &key)
			return key, err == nil
		}
	}
	return string(inner), true
}

func skipJSONSpace(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// validatedStringEnd returns the index after the closing quote of the string
// starting at data[i], or -1 if the string is not terminated.
func validatedStringEnd(data []byte, i int) int {
	for j := i + 1; j < len(data); {
		k := bytes.IndexByte(data[j:], '"')
		if k < 0 {
			return -1
		}
		end := j + k
		backslashes := 0
		for b := end - 1; b > i && data[b] == '\\'; b-- {
			backslashes++
		}
		if backslashes%2 == 0 {
			return end + 1
		}
		j = end + 1
	}
	return -1
}

// validatedValueEnd returns the index after the value starting at data[i], or
// -1 if the value is not terminated.
func validatedValueEnd(data []byte, i int) int {
	if i == len(data) {
		return -1
	}
	switch data[i] {
	case '"':
		return validatedStringEnd(data, i)
	case '{', '[':
		depth := 0
		for j := i; j < len(data); j++ {
			switch data[j] {
			case '"':
				end := validatedStringEnd(data, j)
				if end < 0 {
					return -1
				}
				j = end - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return j + 1
				}
			}
		}
		return -1
	default:
		j := i
		for j < len(data) {
			switch data[j] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				if j == i {
					return -1
				}
				return j
			}
			j++
		}
		return j
	}
}

// Unmarshal decodes one JSON value, preserves numbers as json.Number, and rejects trailing data.
func Unmarshal(data []byte, v any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	switch err := decoder.Decode(&struct{}{}); {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil:
		return ErrTrailingData
	default:
		return fmt.Errorf("%w: %v", ErrTrailingData, err)
	}
}

// UnmarshalMapWithRawField decodes a JSON object while preserving one field's
// original JSON representation. All other numbers are preserved as json.Number.
func UnmarshalMapWithRawField(data []byte, rawField string) (map[string]any, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		if fallbackErr := Unmarshal(data, &fields); fallbackErr != nil {
			return nil, fallbackErr
		}
	}

	result := make(map[string]any, len(fields))
	for key, raw := range fields {
		if key == rawField {
			result[key] = raw
			continue
		}

		var value any
		if err := Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}
