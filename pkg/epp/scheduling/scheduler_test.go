/*
Copyright 2025 The Kubernetes Authors.
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

package scheduling

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/maxscore"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/profilehandler/single"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/kvcacheutilization"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/loraaffinity"
	schedprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/queuedepth"
)

// Tests the default scheduler configuration and expected behavior.
func TestSchedule(t *testing.T) {
	kvCacheUtilizationScorer := kvcacheutilization.NewKVCacheUtilizationScorer()
	queueingScorer := queuedepth.NewQueueScorer()
	prefixCacheScorer, err := schedprefix.New(context.Background(), schedprefix.PrefixCacheScorerPluginType, "approx-prefix-cache-producer")
	assert.NoError(t, err)
	loraAffinityScorer := loraaffinity.NewLoraAffinityScorer()
	datalayer.RegisterScopeSpecs([]fwkplugin.Plugin{
		kvCacheUtilizationScorer, queueingScorer, prefixCacheScorer, loraAffinityScorer,
	})

	defaultProfile := NewSchedulerProfile().
		WithScorers(NewWeightedScorer(kvCacheUtilizationScorer, 1),
			NewWeightedScorer(queueingScorer, 1),
			NewWeightedScorer(prefixCacheScorer, 1),
			NewWeightedScorer(loraAffinityScorer, 1),
		).
		WithPicker(maxscore.NewMaxScorePicker(picker.DefaultMaxNumOfEndpoints))

	profileHandler := single.NewSingleProfileHandler()

	schedulerConfig := NewSchedulerConfig(profileHandler, map[string]fwksched.SchedulerProfile{"default": defaultProfile})
	scrapedAt := time.Now()

	tests := []struct {
		name    string
		req     *fwksched.InferenceRequest
		input   []fwksched.Endpoint
		wantRes *fwksched.SchedulingResult
		err     bool
	}{
		{
			name: "no candidate endpoints",
			req: &fwksched.InferenceRequest{
				RequestID:   uuid.NewString(),
				TargetModel: "any-model",
			},
			input:   []fwksched.Endpoint{},
			wantRes: nil,
			err:     true,
		},
		{
			name: "finds optimal endpoint",
			req: &fwksched.InferenceRequest{
				RequestID:   uuid.NewString(),
				TargetModel: "critical",
			},
			// pod2 will be picked because it has relatively low queue size, with the requested
			// model being active, and has low KV cache.
			input: []fwksched.Endpoint{
				fwksched.NewEndpoint(
					&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod1"}},
					&fwkdl.Metrics{
						WaitingQueueSize:    0,
						KVCacheUsagePercent: 0.2,
						MaxActiveModels:     2,
						ActiveModels: map[string]int{
							"foo": 1,
							"bar": 1,
						},
						UpdateTime: scrapedAt,
					}, nil),
				fwksched.NewEndpoint(
					&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod2"}},
					&fwkdl.Metrics{
						WaitingQueueSize:    0,
						KVCacheUsagePercent: 0.2,
						MaxActiveModels:     2,
						ActiveModels: map[string]int{
							"foo":      1,
							"critical": 1,
						},
						UpdateTime: scrapedAt,
					}, nil),
				fwksched.NewEndpoint(
					&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod3"}},
					&fwkdl.Metrics{
						WaitingQueueSize:    10,
						KVCacheUsagePercent: 0.8,
						MaxActiveModels:     2,
						ActiveModels: map[string]int{
							"foo": 1,
						},
						UpdateTime: scrapedAt,
					}, nil),
			},
			wantRes: &fwksched.SchedulingResult{
				ProfileResults: map[string]*fwksched.ProfileRunResult{
					"default": {
						TargetEndpoints: []fwksched.Endpoint{
							&fwksched.ScoredEndpoint{
								Endpoint: fwksched.NewEndpoint(
									&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod2"}},
									&fwkdl.Metrics{
										WaitingQueueSize:    0,
										KVCacheUsagePercent: 0.2,
										MaxActiveModels:     2,
										ActiveModels: map[string]int{
											"foo":      1,
											"critical": 1,
										},
										UpdateTime: scrapedAt,
									}, nil),
								Score: 2.8,
							},
						},
					},
				},
				PrimaryProfileName: "default",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler := NewSchedulerWithConfig(schedulerConfig)
			got, err := scheduler.Schedule(context.Background(), test.req, test.input)
			if test.err != (err != nil) {
				t.Errorf("Unexpected error, got %v, want %v", err, test.err)
			}

			// ScoredCandidates covers the whole candidate set in unspecified order and
			// is asserted in TestSchedulerProfileScoredCandidates.
			if diff := cmp.Diff(test.wantRes, got, cmp.Comparer(fwksched.ScoredEndpointComparer),
				cmpopts.IgnoreFields(fwksched.ProfileRunResult{}, "ScoredCandidates")); diff != "" {
				t.Errorf("Unexpected output (-want +got): %v", diff)
			}
		})
	}
}

func TestScheduleFilterDrainReturnsTypedError(t *testing.T) {
	drainingFilter := &testPlugin{typedName: fwkplugin.TypedName{Type: "drain-filter", Name: "drain-filter"}} // empty FilterRes drops every endpoint

	profile := NewSchedulerProfile().
		WithFilters(drainingFilter).
		WithPicker(maxscore.NewMaxScorePicker(picker.DefaultMaxNumOfEndpoints))

	schedulerConfig := NewSchedulerConfig(single.NewSingleProfileHandler(), map[string]fwksched.SchedulerProfile{"default": profile})
	scheduler := NewSchedulerWithConfig(schedulerConfig)

	req := &fwksched.InferenceRequest{
		RequestID:   uuid.NewString(),
		TargetModel: "any-model",
	}
	input := []fwksched.Endpoint{
		fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod1"}}, &fwkdl.Metrics{}, nil),
	}

	result, err := scheduler.Schedule(context.Background(), req, input)
	assert.Nil(t, result)
	assert.Error(t, err)

	var typedErr errcommon.Error
	if !errors.As(err, &typedErr) {
		t.Fatalf("Schedule error is not an errcommon.Error: %v", err)
	}
	assert.Equal(t, errcommon.ServiceUnavailable, typedErr.Code)
	assert.Equal(t, string(errcommon.RequestDroppedReasonNoEndpoints), typedErr.Headers[errcommon.RequestDroppedReasonHeaderKey])
}

// bodylessPlugin is a testPlugin that also answers the BodylessCapable query.
// A bare testPlugin does not implement BodylessCapable.
type bodylessPlugin struct {
	testPlugin
	bodyless bool
}

func (p *bodylessPlugin) BodylessCompatible() bool { return p.bodyless }

func newBodylessPlugin(name string, bodyless bool) *bodylessPlugin {
	return &bodylessPlugin{
		testPlugin: testPlugin{
			typedName: fwkplugin.TypedName{Type: "bodyless-test", Name: name},
			PickRes:   k8stypes.NamespacedName{Name: "pod1"},
		},
		bodyless: bodyless,
	}
}

var (
	_ fwkplugin.BodylessCapable = &bodylessPlugin{}
	_ fwksched.Filter           = &bodylessPlugin{}
	_ fwksched.Scorer           = &bodylessPlugin{}
	_ fwksched.Picker           = &bodylessPlugin{}
)

// A profile is bodyless-compatible only when its picker and every filter and
// scorer declare so. A plugin that does not implement BodylessCapable counts as
// incompatible.
func TestSchedulerProfileBodylessCompatible(t *testing.T) {
	tests := []struct {
		name    string
		filters []fwksched.Filter
		scorers []fwksched.Scorer
		picker  fwksched.Picker
		want    bool
	}{
		{
			name:   "compatible picker only",
			picker: newBodylessPlugin("picker", true),
			want:   true,
		},
		{
			name:    "every plugin compatible",
			filters: []fwksched.Filter{newBodylessPlugin("f1", true), newBodylessPlugin("f2", true)},
			scorers: []fwksched.Scorer{newBodylessPlugin("s1", true), newBodylessPlugin("s2", true)},
			picker:  newBodylessPlugin("picker", true),
			want:    true,
		},
		{
			name: "no picker",
			want: false,
		},
		{
			name:   "picker does not implement BodylessCapable",
			picker: &testPlugin{typedName: fwkplugin.TypedName{Type: "plain", Name: "picker"}},
			want:   false,
		},
		{
			name:   "picker reports incompatible",
			picker: newBodylessPlugin("picker", false),
			want:   false,
		},
		{
			name:    "filter does not implement BodylessCapable",
			filters: []fwksched.Filter{newBodylessPlugin("f1", true), &testPlugin{typedName: fwkplugin.TypedName{Type: "plain", Name: "f2"}}},
			picker:  newBodylessPlugin("picker", true),
			want:    false,
		},
		{
			name:    "filter reports incompatible",
			filters: []fwksched.Filter{newBodylessPlugin("f1", true), newBodylessPlugin("f2", false)},
			picker:  newBodylessPlugin("picker", true),
			want:    false,
		},
		{
			name:    "scorer does not implement BodylessCapable",
			scorers: []fwksched.Scorer{newBodylessPlugin("s1", true), &testPlugin{typedName: fwkplugin.TypedName{Type: "plain", Name: "s2"}}},
			picker:  newBodylessPlugin("picker", true),
			want:    false,
		},
		{
			name:    "scorer reports incompatible",
			scorers: []fwksched.Scorer{newBodylessPlugin("s1", true), newBodylessPlugin("s2", false)},
			picker:  newBodylessPlugin("picker", true),
			want:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			weighted := make([]*WeightedScorer, 0, len(test.scorers))
			for _, scorer := range test.scorers {
				weighted = append(weighted, NewWeightedScorer(scorer, 1))
			}
			profile := NewSchedulerProfile().WithFilters(test.filters...).WithScorers(weighted...)
			if test.picker != nil {
				profile.WithPicker(test.picker)
			}

			// The result is computed when the scheduler is built.
			NewSchedulerWithConfig(NewSchedulerConfig(single.NewSingleProfileHandler(), map[string]fwksched.SchedulerProfile{"default": profile}))

			assert.Equal(t, test.want, profile.BodylessCompatible())
		})
	}
}

func TestScheduleBodyless(t *testing.T) {
	const noBodylessProfile = "no bodyless-compatible scheduler profile"

	tests := []struct {
		name          string
		bodyless      bool
		compatible    bool
		wantErr       string
		wantPickCalls int
	}{
		{
			name:          "bodyless request runs a compatible profile",
			bodyless:      true,
			compatible:    true,
			wantPickCalls: 1,
		},
		{
			name:          "bodyless request fails when the only profile is incompatible",
			bodyless:      true,
			compatible:    false,
			wantErr:       noBodylessProfile,
			wantPickCalls: 0,
		},
		{
			name:          "request with a body runs a compatible profile",
			bodyless:      false,
			compatible:    true,
			wantPickCalls: 1,
		},
		{
			name:          "request with a body runs an incompatible profile",
			bodyless:      false,
			compatible:    false,
			wantPickCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			picker := newBodylessPlugin("picker", test.compatible)
			profile := NewSchedulerProfile().WithPicker(picker)
			scheduler := NewSchedulerWithConfig(NewSchedulerConfig(
				single.NewSingleProfileHandler(), map[string]fwksched.SchedulerProfile{"default": profile}))

			req := &fwksched.InferenceRequest{RequestID: uuid.NewString(), TargetModel: "any-model", Bodyless: test.bodyless}
			input := []fwksched.Endpoint{
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod1"}}, &fwkdl.Metrics{}, nil),
			}

			got, err := scheduler.Schedule(context.Background(), req, input)

			assert.Equal(t, test.wantPickCalls, picker.PickCallCount, "picker call count")
			if test.wantErr != "" {
				assert.ErrorContains(t, err, test.wantErr)
				assert.Nil(t, got)
				return
			}
			assert.NoError(t, err)
			if assert.NotNil(t, got) {
				assert.Equal(t, "default", got.PrimaryProfileName)
			}
		})
	}
}

// pickAllOnceHandler picks every profile in the first round and none afterwards.
type pickAllOnceHandler struct {
	primary string
}

func (h *pickAllOnceHandler) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "pick-all-once", Name: "pick-all-once"}
}

func (h *pickAllOnceHandler) Pick(_ context.Context, _ *fwksched.InferenceRequest, profiles map[string]fwksched.SchedulerProfile,
	profileResults map[string]*fwksched.ProfileRunResult) map[string]fwksched.SchedulerProfile {
	if len(profileResults) > 0 {
		return nil
	}
	return profiles
}

func (h *pickAllOnceHandler) ProcessResults(_ context.Context, _ *fwksched.InferenceRequest,
	profileResults map[string]*fwksched.ProfileRunResult) (*fwksched.SchedulingResult, error) {
	return &fwksched.SchedulingResult{ProfileResults: profileResults, PrimaryProfileName: h.primary}, nil
}

// When a bodyless request is offered several profiles and only some are
// compatible, the compatible ones run and the rest are skipped without failing
// the request.
func TestScheduleBodylessSkipsOnlyIncompatibleProfiles(t *testing.T) {
	compatiblePicker := newBodylessPlugin("compatible-picker", true)
	incompatiblePicker := newBodylessPlugin("incompatible-picker", false)
	profiles := map[string]fwksched.SchedulerProfile{
		"compatible":   NewSchedulerProfile().WithPicker(compatiblePicker),
		"incompatible": NewSchedulerProfile().WithPicker(incompatiblePicker),
	}
	scheduler := NewSchedulerWithConfig(NewSchedulerConfig(&pickAllOnceHandler{primary: "compatible"}, profiles))

	req := &fwksched.InferenceRequest{RequestID: uuid.NewString(), TargetModel: "any-model", Bodyless: true}
	input := []fwksched.Endpoint{
		fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod1"}}, &fwkdl.Metrics{}, nil),
	}

	got, err := scheduler.Schedule(context.Background(), req, input)

	assert.NoError(t, err)
	if assert.NotNil(t, got) {
		assert.Equal(t, "compatible", got.PrimaryProfileName)
		assert.Contains(t, got.ProfileResults, "compatible")
		assert.NotContains(t, got.ProfileResults, "incompatible")
	}
	assert.Equal(t, 1, compatiblePicker.PickCallCount, "compatible profile must run")
	assert.Equal(t, 0, incompatiblePicker.PickCallCount, "incompatible profile must be skipped")
}

// messageCaptureSink records the message of every log call at every verbosity.
type messageCaptureSink struct {
	messages []string
}

func (s *messageCaptureSink) Init(_ logr.RuntimeInfo) {}
func (s *messageCaptureSink) Enabled(_ int) bool      { return true }
func (s *messageCaptureSink) Info(_ int, msg string, _ ...any) {
	s.messages = append(s.messages, msg)
}
func (s *messageCaptureSink) Error(_ error, msg string, _ ...any) {
	s.messages = append(s.messages, msg)
}
func (s *messageCaptureSink) WithValues(_ ...any) logr.LogSink { return s }
func (s *messageCaptureSink) WithName(_ string) logr.LogSink   { return s }

func TestScheduleLogsProcessResultsSuccessOnlyWithoutError(t *testing.T) {
	const successMsg = "Completed running profile handler ProcessResults successfully"

	tests := []struct {
		name       string
		filters    []fwksched.Filter
		wantErr    bool
		wantLogged bool
	}{
		{
			name:       "ProcessResults succeeds",
			wantLogged: true,
		},
		{
			name:    "ProcessResults fails",
			filters: []fwksched.Filter{&testPlugin{typedName: fwkplugin.TypedName{Type: "drain-filter", Name: "drain-filter"}}},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := NewSchedulerProfile().
				WithFilters(test.filters...).
				WithPicker(maxscore.NewMaxScorePicker(picker.DefaultMaxNumOfEndpoints))
			scheduler := NewSchedulerWithConfig(NewSchedulerConfig(single.NewSingleProfileHandler(),
				map[string]fwksched.SchedulerProfile{"default": profile}))

			sink := &messageCaptureSink{}
			ctx := log.IntoContext(context.Background(), logr.New(sink))
			req := &fwksched.InferenceRequest{
				RequestID:   uuid.NewString(),
				TargetModel: "any-model",
			}
			input := []fwksched.Endpoint{
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod1"}}, &fwkdl.Metrics{}, nil),
			}

			_, err := scheduler.Schedule(ctx, req, input)
			assert.Equal(t, test.wantErr, err != nil, "Schedule error: %v", err)
			assert.Equal(t, test.wantLogged, slices.Contains(sink.messages, successMsg),
				"success message logged; captured messages: %q", sink.messages)
		})
	}
}
