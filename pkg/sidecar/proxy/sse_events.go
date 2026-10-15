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
	"errors"
	"io"

	"github.com/llm-d/llm-d-router/pkg/common/request"
)

type sseEvent struct {
	raw     []byte
	data    []byte
	hasData bool
}

func parseSSEEvent(raw []byte) sseEvent {
	event := sseEvent{raw: raw}
	var values [][]byte
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSuffix(line, []byte("\r"))
		value, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok && bytes.Equal(line, []byte("data")) {
			ok = true
			value = nil
		}
		if ok {
			event.hasData = true
			values = append(values, bytes.TrimPrefix(value, []byte(" ")))
		}
	}
	event.data = bytes.Join(values, []byte("\n"))
	return event
}

func (e sseEvent) isDone() bool {
	return e.hasData && bytes.Equal(bytes.TrimSpace(e.data), []byte(request.SSEDoneMarker))
}

func (e sseEvent) rewrite(data []byte) []byte {
	var out bytes.Buffer
	replaced := false
	for _, line := range bytes.SplitAfter(e.raw, []byte("\n")) {
		content := bytes.TrimRight(line, "\r\n")
		if bytes.HasPrefix(content, []byte("data:")) || bytes.Equal(content, []byte("data")) {
			if !replaced {
				out.WriteString(request.SSEDataPrefix)
				out.Write(data)
				out.Write(line[len(content):])
				replaced = true
			}
			continue
		}
		out.Write(line)
	}
	return out.Bytes()
}

type sseEventParser struct {
	maxBytes  int
	buffer    []byte
	lineStart int
}

func newSSEEventParser(maxBytes int) *sseEventParser { return &sseEventParser{maxBytes: maxBytes} }

func (p *sseEventParser) write(data []byte, emit func(sseEvent) error) error {
	for len(data) > 0 {
		newline := bytes.IndexByte(data, '\n')
		size := len(data)
		if newline >= 0 {
			size = newline + 1
		}
		if size > p.maxBytes-len(p.buffer) {
			return errors.New("SSE event exceeds the configured byte limit")
		}
		p.buffer = append(p.buffer, data[:size]...)
		data = data[size:]
		if newline < 0 {
			continue
		}
		line := p.buffer[p.lineStart:]
		p.lineStart = len(p.buffer)
		if bytes.Equal(line, []byte("\n")) || bytes.Equal(line, []byte("\r\n")) {
			err := emit(parseSSEEvent(p.buffer))
			p.buffer = p.buffer[:0]
			p.lineStart = 0
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *sseEventParser) finish() error {
	if len(p.buffer) != 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}
