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
	"testing"
)

func TestSSEEventsFragmentedMultiline(t *testing.T) {
	raw := []byte("id: request\r\n: heartbeat\r\ndata: {\"usage\":\r\ndata: {\"num_cached_tokens\":3}}\r\n\r\n")
	parser := newSSEEventParser(4096)
	var events []sseEvent
	for _, b := range raw {
		if err := parser.write([]byte{b}, func(event sseEvent) error {
			events = append(events, sseEvent{raw: bytes.Clone(event.raw), data: bytes.Clone(event.data), hasData: event.hasData})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(events) != 1 || string(events[0].data) != "{\"usage\":\n{\"num_cached_tokens\":3}}" {
		t.Fatalf("events=%+v", events)
	}
	if !bytes.Equal(events[0].raw, raw) {
		t.Fatal("untouched event framing changed")
	}
	updated := events[0].rewrite([]byte(`{"usage":{"num_cached_tokens":2}}`))
	if !bytes.Contains(updated, []byte("id: request\r\n: heartbeat\r\n")) || !bytes.Contains(updated, []byte(`"num_cached_tokens":2`)) || !bytes.HasSuffix(updated, []byte("\r\n\r\n")) {
		t.Fatalf("rewritten=%q", updated)
	}
	if err := parser.finish(); err != nil {
		t.Fatal(err)
	}
}

func TestSSEEventsBoundWithoutDelimiter(t *testing.T) {
	parser := newSSEEventParser(32)
	if err := parser.write(bytes.Repeat([]byte("x"), 33), func(sseEvent) error { return nil }); err == nil {
		t.Fatal("oversized event accepted")
	}
}

func TestSSEEventsTruncation(t *testing.T) {
	parser := newSSEEventParser(1024)
	if err := parser.write([]byte("data: {}\n"), func(sseEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := parser.finish(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("finish=%v", err)
	}
}
