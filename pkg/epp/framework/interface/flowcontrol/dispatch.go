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

package flowcontrol

import "context"

// PrepareDispatchFunc must return promptly. A nil plan means preparation is pending or placement is unavailable.
// Each call must use a fresh placement attempt. An error terminates the queued request.
type PrepareDispatchFunc func(context.Context) (*DispatchPlan, error)

// DispatchPreparer supplies optional request-aware admission. A nil function selects pool-wide admission.
type DispatchPreparer interface {
	DispatchPreparation() PrepareDispatchFunc
}

// DispatchPlan retains a placement until the processor accepts or discards it.
// Commit publishes the placement before the waiting request resumes. It must not block or fail.
type DispatchPlan struct {
	Stages []string
	// Shared pods consume capacity in both pools even when the placement omits remote prefill.
	// An empty reservation set uses Stages.
	ReservationStages []string
	Commit            func()
}

// StageUsageLimitPolicy evaluates all stage signals with one policy tick per dispatch cycle.
// The empty stage name carries the pool-wide signal for requests without a dispatch preparer.
// Output buffers have the same length as priorities and contain 1.0 before the call.
type StageUsageLimitPolicy interface {
	ComputeStageLimits(ctx context.Context, saturation map[string]float64, priorities []int, ceilings map[string][]float64)
}

// StageDispatchReservationTracker limits reservations to the stages a placement uses.
// ReleaseDispatch must release either kind of reservation through the same request identity.
type StageDispatchReservationTracker interface {
	DispatchReservationTracker
	ReserveDispatchStages(requestID string, stages []string) bool
}
