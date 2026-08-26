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

package pipeline

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

type mockStep struct {
	name string
	fn   func(ctx context.Context, rc *RequestContext) error
}

func (m *mockStep) Name() string { return m.name }
func (m *mockStep) Execute(ctx context.Context, rc *RequestContext) error {
	return m.fn(ctx, rc)
}

func TestPipeline_ExecutesStepsInOrder(t *testing.T) {
	order := []string{}
	steps := []Step{
		&mockStep{name: "a", fn: func(_ context.Context, _ *RequestContext) error {
			order = append(order, "a")
			return nil
		}},
		&mockStep{name: "b", fn: func(_ context.Context, _ *RequestContext) error {
			order = append(order, "b")
			return nil
		}},
		&mockStep{name: "c", fn: func(_ context.Context, _ *RequestContext) error {
			order = append(order, "c")
			return nil
		}},
	}

	p := New(steps)
	err := p.Execute(context.Background(), &RequestContext{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("unexpected execution order: %v", order)
	}
}

func TestPipeline_ForwardsConfiguredResponseHeadersBetweenArbitrarySteps(t *testing.T) {
	steps := []Step{
		&mockStep{name: "producer", fn: func(_ context.Context, rc *RequestContext) error {
			if got := rc.ForwardedHeaders()["x-llm-d-disagg-revision"]; got != "" {
				t.Fatalf("client supplied relay header reached first step: %q", got)
			}
			responseHeaders := make(http.Header)
			responseHeaders.Set("X-LLM-D-Disagg-Revision", "revision-b")
			responseHeaders.Set("X-Worker-Only", "worker-value")
			rc.CaptureResponseHeaders(responseHeaders)
			return nil
		}},
		&mockStep{name: "consumer", fn: func(_ context.Context, rc *RequestContext) error {
			headers := rc.ForwardedHeaders()
			if got := headers["x-llm-d-disagg-revision"]; got != "revision-b" {
				t.Fatalf("relayed revision = %q, want %q", got, "revision-b")
			}
			if got := headers["x-worker-only"]; got != "" {
				t.Fatalf("unconfigured response header was relayed: %q", got)
			}
			return nil
		}},
	}

	p, err := NewWithForwardResponseHeaders(steps, []string{" X-LLM-D-Disagg-Revision "})
	if err != nil {
		t.Fatalf("NewWithForwardResponseHeaders() error = %v", err)
	}
	reqCtx := &RequestContext{OriginalHeaders: http.Header{"X-LLM-D-Disagg-Revision": {"client-revision"}}}
	if err := p.Execute(context.Background(), reqCtx); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestPipeline_SelectsMostFrequentValueForEachForwardedHeader(t *testing.T) {
	steps := []Step{
		&mockStep{name: "producer", fn: func(_ context.Context, rc *RequestContext) error {
			responses := []http.Header{
				{"X-Route-One": {"one-b"}, "X-Route-Two": {"two-a"}},
				{"X-Route-One": {"one-a"}, "X-Route-Two": {"two-b"}},
				{"X-Route-One": {"one-a"}, "X-Route-Two": {"two-a"}},
				{"X-Route-One": {"one-b"}},
			}
			rc.CaptureResponseHeaders(responses...)
			return nil
		}},
		&mockStep{name: "consumer", fn: func(_ context.Context, rc *RequestContext) error {
			headers := rc.ForwardedHeaders()
			if got := headers["x-route-one"]; got != "one-b" {
				t.Errorf("tie winner = %q, want first value %q", got, "one-b")
			}
			if got := headers["x-route-two"]; got != "two-a" {
				t.Errorf("majority winner = %q, want %q", got, "two-a")
			}
			return nil
		}},
	}

	p, err := NewWithForwardResponseHeaders(steps, []string{"X-Route-One", "X-Route-Two"})
	if err != nil {
		t.Fatalf("NewWithForwardResponseHeaders() error = %v", err)
	}
	if err := p.Execute(context.Background(), &RequestContext{}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestCaptureResponseHeaders_IgnoresEmptyValues(t *testing.T) {
	rc := &RequestContext{
		forwardResponseHeaders: map[string]struct{}{"x-route": {}},
	}

	rc.CaptureResponseHeaders(
		http.Header{"X-Route": {""}},
		http.Header{"X-Route": {"route-a"}},
		http.Header{"X-Route": {""}},
	)

	if got := rc.downstreamHeaders["x-route"]; got != "route-a" {
		t.Fatalf("captured route = %q, want %q", got, "route-a")
	}
}

func TestNewWithForwardResponseHeaders_RejectsInvalidNames(t *testing.T) {
	for _, headers := range [][]string{
		{""},
		{"x-llm-d-disagg-revision", "X-LLM-D-Disagg-Revision"},
		{"Content-Type"},
	} {
		if _, err := NewWithForwardResponseHeaders(nil, headers); err == nil {
			t.Fatalf("NewWithForwardResponseHeaders(%v) expected error", headers)
		}
	}
}

func TestPipeline_GeneratesRevisionDecisionID(t *testing.T) {
	var decisionID string
	p := New([]Step{&mockStep{name: "capture", fn: func(_ context.Context, rc *RequestContext) error {
		decisionID = rc.RevisionDecisionID
		return nil
	}}})

	if err := p.Execute(context.Background(), &RequestContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decisionID == "" {
		t.Fatal("pipeline did not generate a revision decision ID")
	}
}

func TestPipeline_AbortsOnError(t *testing.T) {
	executed := map[string]bool{}
	steps := []Step{
		&mockStep{name: "a", fn: func(_ context.Context, _ *RequestContext) error {
			executed["a"] = true
			return errors.New("step a failed")
		}},
		&mockStep{name: "b", fn: func(_ context.Context, _ *RequestContext) error {
			executed["b"] = true
			return nil
		}},
	}

	p := New(steps)
	err := p.Execute(context.Background(), &RequestContext{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !executed["a"] {
		t.Fatal("step a should have executed")
	}
	if executed["b"] {
		t.Fatal("step b should NOT have executed")
	}
}

func TestPipeline_StopsOnErrPipelineDone(t *testing.T) {
	executed := map[string]bool{}
	steps := []Step{
		&mockStep{name: "a", fn: func(_ context.Context, _ *RequestContext) error {
			executed["a"] = true
			return ErrPipelineDone
		}},
		&mockStep{name: "b", fn: func(_ context.Context, _ *RequestContext) error {
			executed["b"] = true
			return nil
		}},
	}

	p := New(steps)
	err := p.Execute(context.Background(), &RequestContext{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !executed["a"] {
		t.Fatal("step a should have executed")
	}
	if executed["b"] {
		t.Fatal("step b should NOT have executed after ErrPipelineDone")
	}
}

func TestPipeline_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	steps := []Step{
		&mockStep{name: "a", fn: func(_ context.Context, _ *RequestContext) error {
			t.Fatal("should not execute")
			return nil
		}},
	}

	p := New(steps)
	err := p.Execute(ctx, &RequestContext{})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}
