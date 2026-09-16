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

package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	configapi "github.com/llm-d/llm-d-router/apix/config/v1alpha1"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/profilehandler/dataparallel"
)

func TestRegisterInTreePluginsIncludesDPRankHeaderHandler(t *testing.T) {
	r := NewRunner()
	r.registerInTreePlugins()

	factory, ok := fwkplugin.Registry[dataparallel.DPRankHeaderHandlerType]
	require.True(t, ok)

	p, err := factory("dp-rank", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, dataparallel.DPRankHeaderHandlerType, p.TypedName().Type)
}

func TestValidateDataParallelConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		fallbackSize int
		plugins      []configapi.PluginSpec
		wantError    bool
	}{
		{name: "disabled", fallbackSize: 1},
		{
			name:         "shared port with handler",
			fallbackSize: 8,
			plugins: []configapi.PluginSpec{{
				Type: dataparallel.DPRankHeaderHandlerType,
			}},
		},
		{name: "shared port without handler", fallbackSize: 8, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDataParallelConfiguration(&configapi.EndpointPickerConfig{Plugins: tt.plugins}, tt.fallbackSize)
			if tt.wantError {
				require.ErrorContains(t, err, dataparallel.DPRankHeaderHandlerType)
				return
			}
			require.NoError(t, err)
		})
	}
}
