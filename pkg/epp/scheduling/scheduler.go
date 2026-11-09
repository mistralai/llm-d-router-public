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

// Package scheduling implements request scheduling algorithms.
package scheduling

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/common/observability/semconv"
	"github.com/llm-d/llm-d-router/pkg/common/observability/tracing"
	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

const (
	profilePickerExtensionPoint          = "ProfilePicker"
	filterExtensionPoint                 = "Filter"
	scorerExtensionPoint                 = "Scorer"
	pickerExtensionPoint                 = "Picker"
	processProfilesResultsExtensionPoint = "ProcessProfilesResults"
)

// NewSchedulerWithConfig returns a new scheduler with the given scheduler plugins configuration.
func NewSchedulerWithConfig(config *SchedulerConfig) *Scheduler {
	return &Scheduler{
		profileHandler: config.profileHandler,
		profiles:       config.profiles,
	}
}

type Scheduler struct {
	profileHandler fwksched.ProfileHandler
	profiles       map[string]fwksched.SchedulerProfile
}

// BodylessCompatible reports whether every plugin in the profile can handle a
// request with no body.
func (p *SchedulerProfile) BodylessCompatible() bool {
	for _, filter := range p.filters {
		if bc, ok := filter.(plugin.BodylessCapable); !ok || !bc.BodylessCompatible() {
			return false
		}
	}
	for _, scorer := range p.scorers {
		if bc, ok := scorer.Scorer.(plugin.BodylessCapable); !ok || !bc.BodylessCompatible() {
			return false
		}
	}
	if bc, ok := p.picker.(plugin.BodylessCapable); !ok || !bc.BodylessCompatible() {
		return false
	}
	return true
}

// bodylessCompatible treats a profile that does not implement BodylessCapable
// as incompatible.
func bodylessCompatible(profile fwksched.SchedulerProfile) bool {
	bc, ok := profile.(plugin.BodylessCapable)
	return ok && bc.BodylessCompatible()
}

// Schedule finds the target pod based on metrics and the requested lora adapter.
func (s *Scheduler) Schedule(ctx context.Context, request *fwksched.InferenceRequest, candidateEndpoints []fwksched.Endpoint) (result *fwksched.SchedulingResult, err error) {
	logger := log.FromContext(ctx)
	loggerVerbose := logger.V(logutil.VERBOSE)
	verboseEnabled := loggerVerbose.Enabled()
	handlerName := s.profileHandler.TypedName()

	scheduleStart := time.Now()
	defer func() {
		metrics.RecordSchedulerE2ELatency(time.Since(scheduleStart))
		metrics.RecordSchedulerAttempt(err, request.TargetModel, result)
	}()

	profileRunResults := map[string]*fwksched.ProfileRunResult{}
	// Keyed like profileRunResults so a profile that fails in one iteration and
	// succeeds in a later one leaves no stale error behind. Nil until a profile
	// fails: delete and len are no-ops on a nil map, and the happy path skips
	// the allocation.
	var profileRunErrors map[string]error
	// Counted across rounds: a bodyless request fails here only when no profile ran.
	bodylessSkipped, profilesRun := 0, 0

	for { // get the next set of profiles to run iteratively based on the request and the previous execution results
		if verboseEnabled {
			loggerVerbose.Info("Running profile handler, Pick profiles", "plugin", handlerName)
		}
		// violations dropped: Pick has no error return, so a rejected write is
		// visible only through plugin_data_scope_violations_total. The profiles
		// the handler returns run against the unscoped request below; each
		// filter and scorer is scoped against its own declarations.
		scopedRequest, _ := datalayer.ScopeRequest(logger, profilePickerExtensionPoint, s.profileHandler, request)
		before := time.Now()
		profiles := s.profileHandler.Pick(ctx, scopedRequest, s.profiles, profileRunResults)
		metrics.RecordPluginProcessingLatency(profilePickerExtensionPoint, handlerName.Type, handlerName.Name, time.Since(before))
		if verboseEnabled {
			loggerVerbose.Info("Completed running profile handler Pick profiles successfully", "plugin", handlerName, "result", profiles)
		}
		if len(profiles) == 0 { // profile picker didn't pick any profile to run
			break
		}

		for name, profile := range profiles {
			if request.Bodyless && !bodylessCompatible(profile) {
				loggerVerbose.Info("Skipping bodyless incompatible profile", "profile", name)
				// Nil marks it evaluated so the handler does not pick it again.
				profileRunResults[name] = nil
				bodylessSkipped++
				continue
			}
			profilesRun++
			if verboseEnabled {
				loggerVerbose.Info("Running scheduler profile", "profile", name)
			}
			// run the selected profiles and collect results (current code runs all profiles)
			profileRunResult, err := runSchedulerProfile(ctx, name, profile, request, candidateEndpoints)
			if err != nil {
				if verboseEnabled {
					loggerVerbose.Info("failed to run scheduler profile", "profile", name, "error", err.Error())
				}
				if profileRunErrors == nil {
					profileRunErrors = map[string]error{}
				}
				profileRunErrors[name] = fmt.Errorf("profile %q: %w", name, err)
			} else {
				if verboseEnabled {
					loggerVerbose.Info("Completed running scheduler profile successfully", "profile", name)
				}
				delete(profileRunErrors, name)
			}

			profileRunResults[name] = profileRunResult // if profile failed to run, the run result is nil
		}
	}

	if bodylessSkipped > 0 && profilesRun == 0 {
		err = fmt.Errorf("no bodyless-compatible scheduler profile for request %s", request.RequestID)
		return nil, err
	}

	if len(profileRunResults) == 0 {
		err = fmt.Errorf("failed to run any scheduler profile for request %s", request.RequestID)
		return nil, err
	}

	if verboseEnabled {
		loggerVerbose.Info("Running profile handler, ProcessResults", "plugin", handlerName)
	}
	scopedRequest, violations := datalayer.ScopeRequest(logger, processProfilesResultsExtensionPoint, s.profileHandler, request)
	before := time.Now()
	result, err = s.profileHandler.ProcessResults(ctx, scopedRequest, profileRunResults)
	metrics.RecordPluginProcessingLatency(processProfilesResultsExtensionPoint, handlerName.Type, handlerName.Name, time.Since(before))
	if err == nil {
		err = violations.Write()
	}
	if verboseEnabled && err == nil {
		loggerVerbose.Info("Completed running profile handler ProcessResults successfully", "plugin", handlerName)
	}

	// Profile handlers see failed profiles only as nil results and report them
	// with fresh untyped errors. Join the retained profile errors so a typed
	// errcommon.Error raised inside a profile run (e.g. filters draining the
	// candidate set) stays reachable via errors.As in the caller.
	if err != nil && len(profileRunErrors) > 0 {
		errs := make([]error, 0, len(profileRunErrors)+1)
		errs = append(errs, err)
		// Sorted so error composition, and therefore errors.As selection when
		// profiles fail with different typed codes, is deterministic.
		for _, name := range slices.Sorted(maps.Keys(profileRunErrors)) {
			errs = append(errs, profileRunErrors[name])
		}
		err = errors.Join(errs...)
	}

	return result, err
}

func runSchedulerProfile(ctx context.Context, name string, profile fwksched.SchedulerProfile,
	request *fwksched.InferenceRequest, candidateEndpoints []fwksched.Endpoint,
) (*fwksched.ProfileRunResult, error) {
	profileCtx, span := tracing.Tracer(TracerScope).Start(ctx, "run_scheduler_profile",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(semconv.LLMDEPPProfileName(name)),
	)
	defer span.End()

	return profile.Run(profileCtx, request, candidateEndpoints)
}
