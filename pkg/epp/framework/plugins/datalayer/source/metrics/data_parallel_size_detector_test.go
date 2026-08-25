/*
Copyright 2026 The Kubernetes Authors.

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

package metrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

func TestDataParallelSizeDetector(t *testing.T) {
	tests := []struct {
		name         string
		metrics      string
		wantSize     int
		wantDetected bool
		wantError    string
	}{
		{
			name: "one engine",
			metrics: `# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{model_name="model",engine="0"} 1
`,
			wantSize:     1,
			wantDetected: true,
		},
		{
			name: "eight engines",
			metrics: func() string {
				var result strings.Builder
				result.WriteString("# TYPE vllm:cache_config_info gauge\n")
				for rank := range 8 {
					fmt.Fprintf(&result, "vllm:cache_config_info{model_name=\"model\",engine=\"%d\"} 1\n", rank)
				}
				return result.String()
			}(),
			wantSize:     8,
			wantDetected: true,
		},
		{
			name: "complete topology with partial request activity",
			metrics: func() string {
				var result strings.Builder
				result.WriteString("# TYPE vllm:num_requests_running gauge\n")
				result.WriteString("vllm:num_requests_running{model_name=\"model\",engine=\"0\"} 1\n")
				result.WriteString("# TYPE vllm:cache_config_info gauge\n")
				for rank := range 8 {
					fmt.Fprintf(&result, "vllm:cache_config_info{model_name=\"model\",engine=\"%d\"} 1\n", rank)
				}
				return result.String()
			}(),
			wantSize:     8,
			wantDetected: true,
		},
		{
			name: "duplicate engines across models",
			metrics: `# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{model_name="model-a",engine="0"} 1
vllm:cache_config_info{model_name="model-a",engine="1"} 1
vllm:cache_config_info{model_name="model-b",engine="0"} 1
vllm:cache_config_info{model_name="model-b",engine="1"} 1
`,
			wantSize:     2,
			wantDetected: true,
		},
		{
			name: "request activity metric alone",
			metrics: `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="model",engine="0"} 1
`,
		},
		{
			name: "metric absent",
			metrics: `# TYPE unrelated gauge
unrelated 1
`,
		},
		{
			name: "non-contiguous engines",
			metrics: `# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{model_name="model",engine="0"} 1
vllm:cache_config_info{model_name="model",engine="2"} 1
`,
			wantError: "contiguous",
		},
		{
			name: "invalid engine label",
			metrics: `# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{model_name="model",engine="rank-0"} 1
`,
			wantError: "engine label",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.metrics))
			}))
			defer server.Close()

			detector, err := NewDataParallelSizeDetector(defaultDataParallelSizeMetric)
			require.NoError(t, err)
			endpoint := &fwkdl.EndpointMetadata{
				ID:          types.NamespacedName{Namespace: "default", Name: "pod"},
				Name:        "pod",
				MetricsHost: strings.TrimPrefix(server.URL, "http://"),
			}

			size, detected, err := detector.Detect(context.Background(), endpoint)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSize, size)
			assert.Equal(t, tt.wantDetected, detected)
		})
	}
}
