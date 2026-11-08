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

package handlers

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
)

type heldResponseStream struct {
	mockProcessServer
	ctx       context.Context
	requests  chan *extProcPb.ProcessingRequest
	responses chan *extProcPb.ProcessingResponse
}

func (s *heldResponseStream) Context() context.Context { return s.ctx }

func (s *heldResponseStream) Recv() (*extProcPb.ProcessingRequest, error) {
	select {
	case request := <-s.requests:
		return request, nil
	case <-s.ctx.Done():
		return nil, io.EOF
	}
}

func (s *heldResponseStream) Send(response *extProcPb.ProcessingResponse) error {
	select {
	case s.responses <- response:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func TestRequestBufferReleasedDuringStreamingResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &heldResponseStream{
		ctx:       ctx,
		requests:  make(chan *extProcPb.ProcessingRequest, 2),
		responses: make(chan *extProcPb.ProcessingResponse, 32),
	}
	registry := NewParserRegistry([]fwkrh.Parser{openai.NewOpenAIParser()}, logr.Discard())
	server := NewStreamingServer(nil, &mockDirector{}, registry, 1)
	released := make(chan struct{}, 1)
	server.bufferPool.New = func() any {
		buf := new(bytes.Buffer)
		runtime.SetFinalizer(buf, func(*bytes.Buffer) { released <- struct{}{} })
		return buf
	}
	completed := make(chan error, 1)
	go func() { completed <- server.Process(stream) }()

	stream.requests <- newRequestHeaders(map[string]string{":path": "/v1/chat/completions"})
	body := []byte(`{"model":"test","messages":[{"role":"user","content":"` + strings.Repeat("a", 1<<20) + `"}],"stream":true}`)
	stream.requests <- &extProcPb.ProcessingRequest{Request: &extProcPb.ProcessingRequest_RequestBody{
		RequestBody: &extProcPb.HttpBody{Body: body, EndOfStream: true},
	}}
	forwarded := false
	for !forwarded {
		select {
		case response := <-stream.responses:
			if body := response.GetRequestBody(); body != nil {
				forwarded = body.GetResponse().GetBodyMutation().GetStreamedResponse().GetEndOfStream()
			}
		case err := <-completed:
			t.Fatalf("request ended before forwarding: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("request was not forwarded")
		}
	}

	deadline := time.After(3 * time.Second)
	for {
		runtime.GC()
		select {
		case <-released:
			select {
			case err := <-completed:
				t.Fatalf("stream ended before response: %v", err)
			default:
			}
			cancel()
			select {
			case err := <-completed:
				require.True(t, err == nil || err == context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("stream did not stop after cancellation")
			}
			return
		case <-deadline:
			t.Fatal("request buffer retained while response stream is open")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
