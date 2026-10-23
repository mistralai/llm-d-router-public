/*
Copyright 2026 The Kubernetes Authors.
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

package approximateprefix

import (
	"context"
	"sync"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/prefixhash"
)

var probeStateDataKey = plugin.NewDataKey("probe-state", ApproxPrefixCachePluginType)

func (p *dataProducer) probeStateKey() plugin.DataKey {
	return probeStateDataKey.WithNonEmptyProducerName(p.typedName.Name)
}

func (p *dataProducer) NewProbeDataProducer() requestcontrol.DataProducer {
	return &probeProducer{dataProducer: p, owner: p.TypedName()}
}

func (p *MultiClusterProducer) NewProbeDataProducer() requestcontrol.DataProducer {
	return &probeProducer{dataProducer: p.dataProducer, owner: p.TypedName()}
}

type probeHashInput struct {
	firstPrompt          *fwkrh.PromptTokens
	promptCount          int
	model, salt          string
	blockSize, maxBlocks int
	skip                 bool
}

type probeProducer struct {
	*dataProducer
	owner        plugin.TypedName
	mu           sync.Mutex
	input        probeHashInput
	cached       bool
	hashes       [][]blockHash
	promptTokens []int
}

func (p *probeProducer) TypedName() plugin.TypedName { return p.owner }

func (p *probeProducer) Produce(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	if request == nil {
		return nil
	}
	blockSize := p.GetBlockSize(endpoints)
	hashes, tokens := p.cachedHashes(ctx, request, blockSize)
	return p.produceHashes(ctx, request, endpoints, blockSize, hashes, tokens, true)
}

func (p *probeProducer) cachedHashes(ctx context.Context, request *fwksched.InferenceRequest, blockSize int) ([][]blockHash, []int) {
	input := probeHashInput{model: request.TargetModel, blockSize: blockSize, maxBlocks: p.resolveMaxBlocks(blockSize)}
	if request.Body != nil {
		tokens := request.Body.TokenizedRequest
		if tokens != nil && len(tokens.Prompts) > 0 {
			input.firstPrompt = &tokens.Prompts[0]
			input.promptCount = len(tokens.Prompts)
		}
		input.skip = request.Body.SkipPrefixCacheMatching
		if tokens != nil {
			input.salt = tokens.CacheSalt
		}
	}
	// Tokenized input is immutable while queued. The mutex also protects against timeout overlap.
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.cached || p.input != input {
		p.hashes, p.promptTokens = prefixhash.GetBlockHashesWithPromptTokens(ctx, request, blockSize, input.maxBlocks)
		p.input, p.cached = input, true
	}
	return p.hashes, p.promptTokens
}

func (p *probeProducer) PublishProbe(ctx context.Context, request *fwksched.InferenceRequest) {
	state, ok := fwksched.ReadRequestAttribute[*SchedulingContextState](request, p.probeStateKey())
	if !ok {
		return
	}
	requestID := request.RequestID
	p.pluginState.Write(requestID, plugin.StateKey(p.typedName.Name), state)
	request.PutAttribute(p.probeStateKey(), nil)
	// Admission can fail before PreRequest consumes the published state.
	context.AfterFunc(ctx, func() { p.pluginState.Delete(requestID) })
}
