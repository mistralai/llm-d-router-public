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
	"context"
	"testing"
	"time"

	configPb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcFilterPb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoyTypePb "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

const (
	tunnelModel      = "tunnel-model"
	tunnelFairnessID = "tenant-a"
	tunnelPriority   = 2
	tunnelEndpoint   = "10.0.0.1:8000"
	// tunnelAge is how long the test director pretends the tunnel took to open.
	tunnelAge = 2 * time.Second

	tunnelRequestsMetric   = "llm_d_epp_tunnel_request_total"
	tunnelDurationMetric   = "llm_d_epp_tunnel_request_duration"
	regularRequestsMetric  = "llm_d_epp_request_total"
	completionsRequestPath = "/v1/completions"
)

// tunnelDirector stands in for the Director when driving Process. It schedules
// every request onto a fixed endpoint and counts the hooks Process invokes.
type tunnelDirector struct {
	mockDirector

	bodylessErr error

	bodylessCalls     int
	requestCalls      int
	responseBodyCalls int
	trailCalls        int
	// trailCause is the termination cause the last trail hook observed.
	trailCause fwkrc.TerminationCause
}

func (d *tunnelDirector) HandleBodylessRequest(_ context.Context, reqCtx *RequestContext) (*RequestContext, error) {
	d.bodylessCalls++
	if d.bodylessErr != nil {
		return reqCtx, d.bodylessErr
	}
	d.schedule(reqCtx)
	// Backdated so the tunnel has a measurable duration without sleeping.
	reqCtx.RequestReceivedTimestamp = time.Now().Add(-tunnelAge)
	return reqCtx, nil
}

func (d *tunnelDirector) HandleRequest(_ context.Context, reqCtx *RequestContext, _ *fwkrh.InferenceRequestBody) (*RequestContext, error) {
	d.requestCalls++
	d.schedule(reqCtx)
	return reqCtx, nil
}

func (d *tunnelDirector) HandleResponseBody(_ context.Context, reqCtx *RequestContext, _ bool) *RequestContext {
	d.responseBodyCalls++
	return reqCtx
}

func (d *tunnelDirector) HandleResponseTrail(_ context.Context, reqCtx *RequestContext) *RequestContext {
	d.trailCalls++
	d.trailCause = reqCtx.TerminationCause
	return reqCtx
}

func (d *tunnelDirector) schedule(reqCtx *RequestContext) {
	reqCtx.IncomingModelName = tunnelModel
	reqCtx.TargetModelName = tunnelModel
	reqCtx.Priority = tunnelPriority
	reqCtx.SchedulingRequest = &fwksched.InferenceRequest{FairnessID: tunnelFairnessID}
	reqCtx.TargetPod = &fwkdl.EndpointMetadata{Address: "10.0.0.1", Port: "8000"}
	reqCtx.TargetEndpoint = tunnelEndpoint
}

func newGRPCBidirectionalHeaders() *extProcPb.ProcessingRequest {
	return newRequestHeaders(map[string]string{metadata.GRPCBidirectionalStream: "true"})
}

func newResponseHeaders() *extProcPb.ProcessingRequest {
	return &extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_ResponseHeaders{
			ResponseHeaders: &extProcPb.HttpHeaders{
				Headers: &configPb.HeaderMap{Headers: []*configPb.HeaderValue{
					{Key: "content-type", RawValue: []byte("application/grpc")},
				}},
			},
		},
	}
}

func newResponseTrailers(grpcStatus string) *extProcPb.ProcessingRequest {
	return &extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_ResponseTrailers{
			ResponseTrailers: &extProcPb.HttpTrailers{
				Trailers: &configPb.HeaderMap{Headers: []*configPb.HeaderValue{
					{Key: "grpc-status", RawValue: []byte(grpcStatus)},
				}},
			},
		},
	}
}

// requestHeaderMutations returns the headers a request-headers response sets.
func requestHeaderMutations(resp *extProcPb.ProcessingResponse) map[string]string {
	set := map[string]string{}
	for _, header := range resp.GetRequestHeaders().GetResponse().GetHeaderMutation().GetSetHeaders() {
		set[header.GetHeader().GetKey()] = string(header.GetHeader().GetRawValue())
	}
	return set
}

// counterValue reports a counter's value for the given labels, and zero when
// the series was never created.
func counterValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metricHasLabels(metric, labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func useCleanMetrics(t *testing.T) {
	t.Helper()

	eppmetrics.Register()
	eppmetrics.Reset()
	t.Cleanup(eppmetrics.Reset)
}

// runTunnel drives Process over the given messages and returns the server that
// recorded what Process sent back.
func runTunnel(t *testing.T, director Director, reqs ...*extProcPb.ProcessingRequest) *scriptedProcessServer {
	t.Helper()

	useCleanMetrics(t)
	srv := &scriptedProcessServer{ctx: context.Background(), reqs: reqs}
	registry := NewParserRegistry([]fwkrh.Parser{openai.NewOpenAIParser()}, logr.Discard())
	require.NoError(t, NewStreamingServer(nil, director, registry, 0).Process(srv))
	return srv
}

func tunnelLabels() map[string]string {
	return map[string]string{
		"model_name":        tunnelModel,
		"target_model_name": tunnelModel,
		"fairness_id":       tunnelFairnessID,
		"priority":          "2",
	}
}

func TestProcessGRPCBidirectionalTunnel(t *testing.T) {
	tests := []struct {
		name       string
		grpcStatus string
		wantCause  fwkrc.TerminationCause
	}{
		{
			name:       "trailers report success",
			grpcStatus: "0",
		},
		{
			name:       "trailers report a gRPC error",
			grpcStatus: "13",
			wantCause:  fwkrc.TerminationCauseError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			director := &tunnelDirector{}
			srv := runTunnel(t, director,
				newGRPCBidirectionalHeaders(),
				newResponseHeaders(),
				newResponseTrailers(tt.grpcStatus),
			)

			require.Len(t, srv.sentResponses, 3)
			reqHeaders, respHeaders, respTrailers := srv.sentResponses[0], srv.sentResponses[1], srv.sentResponses[2]

			require.NotNil(t, reqHeaders.GetRequestHeaders())
			assert.Equal(t, tunnelEndpoint, requestHeaderMutations(reqHeaders)[metadata.DestinationEndpointKey])
			mode := reqHeaders.GetModeOverride()
			require.NotNil(t, mode, "the tunnel must switch off body processing and ask for trailers")
			assert.Equal(t, extProcFilterPb.ProcessingMode_NONE, mode.GetRequestBodyMode())
			assert.Equal(t, extProcFilterPb.ProcessingMode_NONE, mode.GetResponseBodyMode())
			assert.Equal(t, extProcFilterPb.ProcessingMode_SEND, mode.GetResponseTrailerMode())
			assert.NotNil(t, respHeaders.GetResponseHeaders())
			assert.NotNil(t, respTrailers.GetResponseTrailers())

			assert.Equal(t, 1, director.bodylessCalls)
			assert.Zero(t, director.requestCalls, "a tunnel has no body to parse")
			assert.Zero(t, director.responseBodyCalls)
			assert.Equal(t, 1, director.trailCalls, "the trailers end the tunnel exactly once")
			assert.Equal(t, tt.wantCause, director.trailCause)

			assert.Equal(t, 1.0, counterValue(t, tunnelRequestsMetric, tunnelLabels()))
			assert.Zero(t, counterValue(t, regularRequestsMetric, nil), "a tunnel is not counted as a regular request")
			duration := findHistogramMetric(t, tunnelDurationMetric, tunnelLabels())
			assert.Equal(t, uint64(1), duration.GetSampleCount())
			assert.GreaterOrEqual(t, duration.GetSampleSum(), tunnelAge.Seconds())
		})
	}
}

// Without trailers the stream ends on a disconnect or error, so the cleanup
// path runs the trail hook instead of the response body hook.
func TestProcessBodylessAbortWithoutTrailers(t *testing.T) {
	director := &tunnelDirector{}
	runTunnel(t, director,
		newGRPCBidirectionalHeaders(),
		newResponseHeaders(),
	)

	assert.Equal(t, 1, director.trailCalls)
	assert.Zero(t, director.responseBodyCalls)
	assert.Equal(t, fwkrc.TerminationCauseError, director.trailCause)
}

func TestProcessRegularRequestAbortRunsResponseBodyHook(t *testing.T) {
	director := &tunnelDirector{}
	srv := runTunnel(t, director,
		newRequestHeaders(map[string]string{":path": completionsRequestPath}),
		&extProcPb.ProcessingRequest{Request: &extProcPb.ProcessingRequest_RequestBody{RequestBody: &extProcPb.HttpBody{
			Body:        []byte(`{"model":"m","prompt":"hi"}`),
			EndOfStream: true,
		}}},
	)

	assert.Equal(t, 1, director.requestCalls)
	assert.Zero(t, director.bodylessCalls)
	assert.Equal(t, 1, director.responseBodyCalls)
	assert.Zero(t, director.trailCalls)
	require.NotEmpty(t, srv.sentResponses)
	assert.Nil(t, srv.sentResponses[0].GetModeOverride(), "regular requests keep the proxy's processing mode")
	assert.Zero(t, counterValue(t, tunnelRequestsMetric, nil))
}

func TestProcessBodylessSchedulingErrorSendsImmediateResponse(t *testing.T) {
	director := &tunnelDirector{bodylessErr: errcommon.Error{Code: errcommon.ServiceUnavailable, Msg: "no endpoints"}}
	srv := runTunnel(t, director, newGRPCBidirectionalHeaders())

	require.Len(t, srv.sentResponses, 1)
	immediate := srv.sentResponses[0].GetImmediateResponse()
	require.NotNil(t, immediate)
	assert.Equal(t, envoyTypePb.StatusCode_ServiceUnavailable, immediate.GetStatus().GetCode())
	assert.Equal(t, 1, director.bodylessCalls)
	assert.Zero(t, director.trailCalls, "a request that was never scheduled has nothing to release")
	assert.Zero(t, counterValue(t, tunnelRequestsMetric, nil))
}

func TestFinishTunnel(t *testing.T) {
	tests := []struct {
		name              string
		grpcBidirectional bool
		wantTrailCalls    int
		wantDurations     uint64
	}{
		{
			name:              "bidirectional stream runs the trail hook and records its duration",
			grpcBidirectional: true,
			wantTrailCalls:    1,
			wantDurations:     1,
		},
		{
			name: "other bodyless request is only marked complete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useCleanMetrics(t)
			director := &tunnelDirector{}
			server := &StreamingServer{director: director}
			reqCtx := &RequestContext{
				bodyless:                 true,
				grpcBidirectional:        tt.grpcBidirectional,
				IncomingModelName:        tunnelModel,
				TargetModelName:          tunnelModel,
				RequestReceivedTimestamp: time.Now().Add(-time.Minute),
			}
			labels := map[string]string{
				"model_name":  tunnelModel,
				"fairness_id": reqcommon.DefaultFairnessID,
				"priority":    "0",
			}

			server.finishTunnel(context.Background(), reqCtx)

			assert.True(t, reqCtx.responseComplete)
			assert.Equal(t, tt.grpcBidirectional, !reqCtx.responseCompleteTimestamp.IsZero())
			assert.Equal(t, tt.wantTrailCalls, director.trailCalls)
			assert.Equal(t, tt.wantDurations, histogramSampleCount(t, tunnelDurationMetric, labels))

			server.finishTunnel(context.Background(), reqCtx)

			assert.Equal(t, tt.wantTrailCalls, director.trailCalls, "finishing twice must not repeat the side effects")
			assert.Equal(t, tt.wantDurations, histogramSampleCount(t, tunnelDurationMetric, labels))
		})
	}
}

func TestHandleResponseTrail(t *testing.T) {
	received := time.Now()

	tests := []struct {
		name          string
		completedAt   time.Time
		wantDurations uint64
		wantSum       float64
	}{
		{
			name:          "records the tunnel duration",
			completedAt:   received.Add(10 * time.Second),
			wantDurations: 1,
			wantSum:       10,
		},
		{
			name: "records nothing when the completion time was never set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useCleanMetrics(t)
			director := &tunnelDirector{}
			server := &StreamingServer{director: director}
			reqCtx := &RequestContext{
				IncomingModelName:         tunnelModel,
				TargetModelName:           tunnelModel,
				Priority:                  tunnelPriority,
				SchedulingRequest:         &fwksched.InferenceRequest{FairnessID: tunnelFairnessID},
				RequestReceivedTimestamp:  received,
				responseCompleteTimestamp: tt.completedAt,
			}

			got := server.HandleResponseTrail(context.Background(), reqCtx)

			assert.Same(t, reqCtx, got)
			assert.Equal(t, 1, director.trailCalls, "the director hook runs even when no duration is recorded")
			assert.Equal(t, tt.wantDurations, histogramSampleCount(t, tunnelDurationMetric, tunnelLabels()))
			if tt.wantDurations > 0 {
				assert.Equal(t, tt.wantSum, findHistogramMetric(t, tunnelDurationMetric, tunnelLabels()).GetSampleSum())
			}
		})
	}
}
