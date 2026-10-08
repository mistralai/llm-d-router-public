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

package datalayer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type bindingValue int

func (v bindingValue) Clone() Cloneable { return v }

func TestStateBindingsKeepsCallbacksForEachKey(t *testing.T) {
	var bindings StateBindings
	local := map[string]bindingValue{"endpoint-a": 4, "endpoint-b": 5}
	bindings.Bind("load", func(id string) Cloneable { return local[id] }, func(values []any) any {
		var total bindingValue
		for _, value := range values {
			total += value.(bindingValue)
		}
		return total
	})
	bindings.Bind("other", func(string) Cloneable { return bindingValue(20) }, func(values []any) any {
		return values[0].(bindingValue) * 2
	})

	load, err := bindings.Binding("load")
	require.NoError(t, err)
	assert.Equal(t, bindingValue(11), load.Aggregate([]any{load.Read("endpoint-a"), bindingValue(7)}))
	assert.Equal(t, bindingValue(5), load.Read("endpoint-b"))

	local["endpoint-a"] = 0
	assert.Equal(t, bindingValue(7), load.Aggregate([]any{load.Read("endpoint-a"), bindingValue(7)}))

	other, err := bindings.Binding("other")
	require.NoError(t, err)
	assert.Equal(t, bindingValue(40), other.Aggregate([]any{other.Read("endpoint-a")}))
}

func TestStateBindingsRejectsUnboundKeys(t *testing.T) {
	var first, second StateBindings
	_, err := first.Binding("missing")
	require.ErrorContains(t, err, "missing")

	first.Bind("load", func(string) Cloneable { return bindingValue(1) }, func(values []any) any {
		return values[0]
	})
	_, err = second.Binding("load")
	require.ErrorContains(t, err, "load")
}
