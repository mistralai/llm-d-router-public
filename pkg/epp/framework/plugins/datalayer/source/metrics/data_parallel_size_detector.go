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
	"errors"
	"fmt"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	sourcehttp "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/http"
)

const (
	defaultDataParallelSizeMetric = "vllm:cache_config_info"
	dataParallelEngineLabel       = "engine"
	dataParallelDetectionTimeout  = 2 * time.Second
)

// DataParallelSizeDetector discovers the logical engines served by one model-server endpoint.
type DataParallelSizeDetector struct {
	source     *sourcehttp.HTTPDataSource[PrometheusMetricMap]
	metricName string
}

// NewDataParallelSizeDetector constructs a detector using the model server's /metrics endpoint.
func NewDataParallelSizeDetector(metricName string) (*DataParallelSizeDetector, error) {
	if metricName == "" {
		metricName = defaultDataParallelSizeMetric
	}
	source, err := NewHTTPMetricsDataSource(defaultMetricsScheme, defaultMetricsPath, "data-parallel-size-detector")
	if err != nil {
		return nil, err
	}
	return &DataParallelSizeDetector{source: source, metricName: metricName}, nil
}

// Detect returns the number of contiguous engine labels exposed by the configured metric.
func (d *DataParallelSizeDetector) Detect(ctx context.Context, endpoint *fwkdl.EndpointMetadata) (int, bool, error) {
	if endpoint == nil {
		return 0, false, errors.New("endpoint metadata is required")
	}
	detectCtx, cancel := context.WithTimeout(ctx, dataParallelDetectionTimeout)
	defer cancel()

	families, err := d.source.Poll(detectCtx, fwkdl.NewEndpoint(endpoint, fwkdl.NewMetrics()))
	if err != nil {
		return 0, false, err
	}
	size, detected, err := dataParallelSizeFromMetrics(families, d.metricName)
	if err != nil {
		return 0, false, fmt.Errorf("detect data-parallel size for %s: %w", endpoint.ID, err)
	}
	return size, detected, nil
}

func dataParallelSizeFromMetrics(families PrometheusMetricMap, metricName string) (int, bool, error) {
	family := families[metricName]
	if family == nil {
		return 0, false, nil
	}

	engines := sets.New[int]()
	for _, metric := range family.GetMetric() {
		engineLabel := ""
		for _, label := range metric.GetLabel() {
			if label.GetName() == dataParallelEngineLabel {
				engineLabel = label.GetValue()
				break
			}
		}
		if engineLabel == "" {
			return 0, false, fmt.Errorf("metric %q has no %q label", metricName, dataParallelEngineLabel)
		}
		engine, err := strconv.Atoi(engineLabel)
		if err != nil || engine < 0 {
			return 0, false, fmt.Errorf("metric %q has invalid engine label %q", metricName, engineLabel)
		}
		engines.Insert(engine)
	}
	if engines.Len() == 0 {
		return 0, false, nil
	}
	for engine := range engines.Len() {
		if !engines.Has(engine) {
			return 0, false, fmt.Errorf("metric %q engine labels must be contiguous from zero", metricName)
		}
	}
	return engines.Len(), true, nil
}
