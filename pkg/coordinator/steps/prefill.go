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

package steps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"

	"github.com/llm-d/llm-d-router/pkg/coordinator/connectors/ec"
	"github.com/llm-d/llm-d-router/pkg/coordinator/connectors/kv"
	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	coordmetrics "github.com/llm-d/llm-d-router/pkg/coordinator/metrics"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

const PrefillStepName = "prefill"

func init() {
	pipeline.Register(PrefillStepName, NewPrefillStep)
}

type PrefillStep struct {
	useOpenAIFormat     bool
	reusePromptTokenIDs bool
	gwClient            *gateway.Client
	kv                  kv.Connector
	ec                  ec.Connector
}

func NewPrefillStep(gwClient *gateway.Client, params map[string]any) (pipeline.Step, error) {
	if gwClient == nil {
		return nil, errors.New("prefill: gateway client is required")
	}
	useOpenAI, err := parseUseOpenAIFormat(params)
	if err != nil {
		return nil, fmt.Errorf("prefill: %w", err)
	}
	reusePromptTokenIDs, _, err := paramBool(params, "reuse_prompt_token_ids")
	if err != nil {
		return nil, fmt.Errorf("prefill: %w", err)
	}
	kvConn, err := buildKVConnector(params)
	if err != nil {
		return nil, fmt.Errorf("prefill: %w", err)
	}
	if reusePromptTokenIDs && (!useOpenAI || kvConn.Name() != kv.NIXL) {
		return nil, errors.New("prefill: reuse_prompt_token_ids requires use_openai_format=true and kv_connector=kv-nixl")
	}
	ecConn, err := buildECConnector(params)
	if err != nil {
		return nil, fmt.Errorf("prefill: %w", err)
	}
	return &PrefillStep{
		useOpenAIFormat:     useOpenAI,
		reusePromptTokenIDs: reusePromptTokenIDs,
		gwClient:            gwClient,
		kv:                  kvConn,
		ec:                  ecConn,
	}, nil
}

func (s *PrefillStep) Name() string { return PrefillStepName }

func (s *PrefillStep) Execute(ctx context.Context, reqCtx *pipeline.RequestContext) error {
	logger := log.FromContext(ctx).WithName(PrefillStepName)

	format := resolveFormat(s.useOpenAIFormat, reqCtx.OriginalPath)
	if s.reusePromptTokenIDs {
		if format != reqcommon.APITypeChatCompletions {
			return fmt.Errorf("prefill: reuse_prompt_token_ids supports only text chat completions: %w", pipeline.ErrBadRequest)
		}
		if len(reqCtx.TokenIDs) != 0 {
			return errors.New("prefill: reuse_prompt_token_ids requires prefill to tokenize; remove the render step")
		}
		if err := validateTextChatForTokenReuse(reqCtx.Body); err != nil {
			return fmt.Errorf("prefill: %w", err)
		}
	}
	body, err := s.buildPrefillBody(ctx, reqCtx, format)
	if err != nil {
		return fmt.Errorf("prefill: %w", err)
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("prefill: marshal: %w", err)
	}

	path := format.Path()
	logger.V(logutil.DEFAULT).Info("sending request", "path", path)

	resp, err := postToGateway(ctx, logger, s.gwClient, gatewayRequest{
		logMsg:   "request body",
		step:     PrefillStepName,
		upstream: coordmetrics.UpstreamPrefill,
		path:     path,
		body:     bodyBytes,
		headers:  gatewayHeaders(reqCtx, gateway.PhasePrefill),
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	reqCtx.PeerTopology = resp.Header.Get(reqcommon.PeerTopologyHeaderKey)

	var prefillResp prefillResponse
	if err := json.NewDecoder(resp.Body).Decode(&prefillResp); err != nil {
		return fmt.Errorf("prefill: decode response: %w", err)
	}

	reqCtx.KVTransferParams = coerceParamsMap(logger, prefillResp.KVTransferParams, "kv_transfer_params")
	if len(reqCtx.KVTransferParams) == 0 && s.kv.Name() == kv.NIXL {
		// kv-nixl always requests a remote-decode handoff, so missing
		// kv_transfer_params means decode will recompute the whole prompt.
		// Older engines (< v0.29.0) silently drop top-level transfer params
		// on this route, which is the most common cause.
		logger.Info("prefill returned no kv_transfer_params; decode will recompute the prompt",
			"kvConnector", s.kv.Name(), "path", path)
	}
	if s.reusePromptTokenIDs {
		var tokenIDs []int
		if err := json.Unmarshal(prefillResp.PromptTokenIDs, &tokenIDs); err != nil {
			return fmt.Errorf("prefill: invalid prompt_token_ids in prefill response: %w", err)
		}
		if len(tokenIDs) == 0 || len(reqCtx.KVTransferParams) == 0 {
			return errors.New("prefill: token reuse requires non-empty prompt_token_ids and kv_transfer_params in prefill response")
		}
		for _, id := range tokenIDs {
			if id < 0 {
				return fmt.Errorf("prefill: prefill response contains negative prompt token ID %d", id)
			}
		}
		// The NIXL connector forwards this map to D's chat endpoint, which
		// consumes the IDs before templating and prompt tokenization.
		reqCtx.KVTransferParams["prompt_token_ids"] = tokenIDs
	}
	reqCtx.CaptureResponseHeaders(resp.Header)

	logger.V(logutil.DEFAULT).Info("complete")
	return nil
}

func (s *PrefillStep) buildPrefillBody(ctx context.Context, reqCtx *pipeline.RequestContext, format reqcommon.APIType) (map[string]any, error) {
	ecParams, err := s.ec.PreparePrefillECParams(ctx, reqCtx)
	if err != nil {
		return nil, err
	}
	kvParams := s.kv.PreparePrefillKVParams(ctx, reqCtx)

	switch format {
	case reqcommon.APITypeChatCompletions, reqcommon.APITypeResponses:
		body := maps.Clone(reqCtx.Body)
		reqcommon.CapSingleToken(body, format)
		if s.reusePromptTokenIDs {
			body["return_token_ids"] = true
		}
		body[reqcommon.FieldKVTransferParams] = kvParams
		if len(ecParams) > 0 {
			body[reqcommon.FieldECTransferParams] = ecParams
		}
		return body, nil

	case reqcommon.APITypeCompletions:
		prompt := reqCtx.Body["prompt"]
		if len(reqCtx.TokenIDs) > 0 {
			prompt = reqCtx.TokenIDs
		}
		body := map[string]any{
			"request_id":                    reqCtx.RequestID,
			"model":                         reqCtx.Model,
			"prompt":                        prompt,
			reqcommon.FieldKVTransferParams: kvParams,
		}
		reqcommon.CapSingleToken(body, format)
		if features := buildMMFeatures(reqCtx.MultimodalEntries, true); features != nil {
			body["features"] = features
		}
		if len(ecParams) > 0 {
			body[reqcommon.FieldECTransferParams] = ecParams
		}
		return body, nil

	case reqcommon.APITypeVLLMGenerate:
		body := map[string]any{
			"request_id":                    reqCtx.RequestID,
			"token_ids":                     reqCtx.TokenIDs,
			"model":                         reqCtx.Model,
			reqcommon.FieldKVTransferParams: kvParams,
		}
		reqcommon.CapSingleToken(body, format)
		if features := buildMMFeatures(reqCtx.MultimodalEntries, true); features != nil {
			body["features"] = features
		}
		if len(ecParams) > 0 {
			body[reqcommon.FieldECTransferParams] = ecParams
		}
		return body, nil

	default:
		// resolveFormat above never returns anything but the formats handled
		// here, so this case is unreachable unless resolveFormat's contract
		// changes.
		return nil, unreachableFormatError(format)
	}
}

type prefillResponse struct {
	// KVTransferParams is decoded as any (not map[string]any) so a non-object
	// value does not fail the decode; coerceParamsMap coerces it.
	KVTransferParams any             `json:"kv_transfer_params"`
	PromptTokenIDs   json.RawMessage `json:"prompt_token_ids"`
}
