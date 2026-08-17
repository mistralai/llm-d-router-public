/*
Copyright 2025 The Kubernetes Authors.

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

package server

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/llm-d/llm-d-router/pkg/common"
)

func TestDefaultManagerOptionsReleaseLeaderWithoutShutdownDelay(t *testing.T) {
	cfg := NewControllerConfig(false)
	gknn := common.GKNN{NamespacedName: types.NamespacedName{Namespace: "test", Name: "pool"}}
	opts := defaultManagerOptions(cfg, gknn, metricsserver.Options{}, NewScheme(cfg))

	if opts.GracefulShutdownTimeout == nil {
		t.Fatal("GracefulShutdownTimeout is nil, want zero for prompt lease release")
	}
	if got := *opts.GracefulShutdownTimeout; got != time.Duration(0) {
		t.Fatalf("GracefulShutdownTimeout = %s, want 0", got)
	}
}
