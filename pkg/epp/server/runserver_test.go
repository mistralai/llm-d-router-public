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

package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/server"
)

func TestRunnable(t *testing.T) {
	// Make sure AsRunnable() does not use leader election.
	runner := server.NewDefaultExtProcServerRunner().AsRunnable(logutil.NewTestLogger())
	r, ok := runner.(manager.LeaderElectionRunnable)
	if !ok {
		t.Fatal("runner is not LeaderElectionRunnable")
	}
	if r.NeedLeaderElection() {
		t.Error("runner returned NeedLeaderElection = true, expected false")
	}
}

func TestExtProcServerUsesConfiguredHealthServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	healthServer := health.NewServer()
	healthServer.SetServingStatus("envoy.service.ext_proc.v3.ExternalProcessor", healthgrpc.HealthCheckResponse_NOT_SERVING)

	runner := server.NewDefaultExtProcServerRunner()
	runner.GrpcListener = listener
	runner.SecureServing = false
	runner.HealthChecking = true
	runner.HealthServer = healthServer

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.AsRunnable(logutil.NewTestLogger()).Start(ctx)
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	checkCtx, cancelCheck := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCheck()
	resp, err := healthgrpc.NewHealthClient(conn).Check(checkCtx, &healthgrpc.HealthCheckRequest{Service: "envoy.service.ext_proc.v3.ExternalProcessor"})
	if err != nil {
		cancel()
		t.Fatalf("health check: %v", err)
	}
	if got := resp.GetStatus(); got != healthgrpc.HealthCheckResponse_NOT_SERVING {
		cancel()
		t.Fatalf("health status = %s, want NOT_SERVING", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("server stopped with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop")
	}
}
