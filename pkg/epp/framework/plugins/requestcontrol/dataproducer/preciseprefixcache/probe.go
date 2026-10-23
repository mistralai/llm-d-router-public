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

package preciseprefixcache

import (
	"context"
	"sync"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func (p *Producer) NewProbeDataProducer() requestcontrol.DataProducer {
	return &probeProducer{Producer: p}
}

type probeKeyInput struct {
	firstPrompt *fwkrh.PromptTokens
	promptCount int
	model, salt string
	blockSize   int
	skip        bool
}

type probeProducer struct {
	*Producer
	mu      sync.Mutex
	input   probeKeyInput
	cached  bool
	keys    [][]kvblock.BlockHash
	content []*mmPromptContent
}

func (p *probeProducer) Produce(ctx context.Context, request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) error {
	return p.produce(ctx, request, endpoints, p)
}

func (p *probeProducer) cachedBlockKeys(ctx context.Context, request *scheduling.InferenceRequest) ([][]kvblock.BlockHash, []*mmPromptContent, error) {
	if request == nil {
		return nil, nil, nil
	}
	input := probeKeyInput{model: request.TargetModel, blockSize: p.blockSizeTokens}
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
	// Cache only immutable input-derived keys. Index matches are refreshed on every probe.
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.cached || p.input != input {
		keys, content, err := computeBlockKeys(ctx, p.kvCacheIndexer, request, p.blockSizeTokens)
		if err != nil {
			return nil, nil, err
		}
		p.keys, p.content, p.input, p.cached = keys, content, input, true
	}
	return p.keys, p.content, nil
}
