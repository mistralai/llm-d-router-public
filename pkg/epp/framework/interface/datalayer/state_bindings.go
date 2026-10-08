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

import "fmt"

// StateBindings supplies CrossReplicaSyncer.Bind when embedded in a syncer.
// The zero value is ready for use.
type StateBindings struct {
	states map[StateKey]StateBinding
}

// StateBinding holds the callbacks registered for a contributor's state key.
type StateBinding struct {
	Read      func(endpointID string) Cloneable
	Aggregate func([]any) any
}

// Bind implements CrossReplicaSyncer.Bind.
func (b *StateBindings) Bind(key StateKey, read func(string) Cloneable, aggregate func([]any) any) {
	if b.states == nil {
		b.states = make(map[StateKey]StateBinding)
	}
	b.states[key] = StateBinding{Read: read, Aggregate: aggregate}
}

// Binding returns the callbacks registered for key.
func (b *StateBindings) Binding(key StateKey) (StateBinding, error) {
	state, ok := b.states[key]
	if !ok {
		return StateBinding{}, fmt.Errorf("state key %q is not bound", key)
	}
	return state, nil
}
