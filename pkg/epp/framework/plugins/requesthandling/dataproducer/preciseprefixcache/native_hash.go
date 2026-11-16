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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkrequest "github.com/llm-d/llm-d-router/pkg/epp/framework/common/request"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/dataproducer/tokenizer"
	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvevents"
)

const maxHashResponseBytes = 16 << 20

type nativeHashClient struct {
	url    string
	client *http.Client
}

type nativeHashGroup struct {
	GroupIdx  int                      `json:"group_idx"`
	Kind      kvevents.KVCacheSpecKind `json:"kind"`
	BlockSize int                      `json:"block_size"`
}

type nativeHashPrompt struct {
	NumTokens   int      `json:"num_tokens"`
	BlockHashes []string `json:"block_hashes"`
	hashes      []uint64
}

type nativeHashPlaceholder struct {
	Offset int `json:"offset"`
	Length int `json:"length"`
}

type nativeHashResponse struct {
	SchemaVersion int                `json:"schema_version"`
	Model         string             `json:"model"`
	HashAlgorithm string             `json:"hash_algorithm"`
	HashEncoding  string             `json:"hash_encoding"`
	HashBlockSize int                `json:"hash_block_size"`
	Lookahead     int                `json:"num_hash_lookahead_tokens"`
	Groups        []nativeHashGroup  `json:"groups"`
	Prompts       []nativeHashPrompt `json:"prompts"`
}

func newNativeHashClient(endpoint, timeout string) (*nativeHashClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return nil, errors.New("hashEndpoint must be an absolute HTTP(S) URL without credentials or fragment")
	}
	duration := 5 * time.Second
	if timeout != "" {
		duration, err = time.ParseDuration(timeout)
		if err != nil || duration <= 0 {
			return nil, errors.New("hashTimeout must be a positive duration")
		}
	}
	return &nativeHashClient{url: u.String(), client: &http.Client{
		Timeout:       duration,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func nativeHashPayloads(request *scheduling.InferenceRequest) ([][]byte, error) {
	body := request.Body
	if tokens := body.TokenizedRequest; tokens != nil {
		if tokens.LegacyTranslated {
			return nil, errors.New("native hashing requires engine-aligned tokens")
		}
		payloads := make([][]byte, 0, len(tokens.Prompts))
		for _, prompt := range tokens.Prompts {
			payload := map[string]any{"model": request.TargetModel, "token_ids": prompt.TokenIDs}
			if tokens.CacheSalt != "" {
				payload["cache_salt"] = tokens.CacheSalt
			}
			if len(prompt.MultiModalFeatures) > 0 {
				hashes, positions := tokenizer.ConvertMMFeaturesFromUpstream(prompt.MultiModalFeatures)
				placeholders := make(map[string][]nativeHashPlaceholder, len(positions))
				for modality, ranges := range positions {
					for _, position := range ranges {
						placeholders[modality] = append(placeholders[modality], nativeHashPlaceholder{Offset: position.Offset, Length: position.Length})
					}
				}
				payload["features"] = map[string]any{"mm_hashes": hashes, "mm_placeholders": placeholders}
			}
			data, err := json.Marshal(payload)
			if err != nil {
				return nil, err
			}
			payloads = append(payloads, data)
		}
		return payloads, nil
	}
	if body.Messages != nil || body.Responses != nil {
		return nil, errors.New("native hashing supports OpenAI chat, completions and token inputs")
	}
	switch payload := body.WirePayload().(type) {
	case fwkrequest.RawPayload:
		return [][]byte{payload}, nil
	case fwkrequest.Marshaler:
		data, err := payload.Marshal()
		return [][]byte{data}, err
	default:
		return nil, errors.New("native hashing requires a JSON request payload")
	}
}

func (c *nativeHashClient) hash(ctx context.Context, request *scheduling.InferenceRequest) (*nativeHashResponse, error) {
	payloads, err := nativeHashPayloads(request)
	if err != nil || len(payloads) == 0 {
		return nil, err
	}
	var result *nativeHashResponse
	for _, payload := range payloads {
		part, err := c.call(ctx, payload, request.Headers)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = part
			continue
		}
		if result.Model != part.Model || result.HashAlgorithm != part.HashAlgorithm || result.HashBlockSize != part.HashBlockSize ||
			result.Lookahead != part.Lookahead || !reflect.DeepEqual(result.Groups, part.Groups) {
			return nil, errors.New("hash configuration changed between prompts")
		}
		result.Prompts = append(result.Prompts, part.Prompts...)
	}
	return result, nil
}

func (c *nativeHashClient) call(ctx context.Context, payload []byte, headers map[string]string) (*nativeHashResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		if strings.EqualFold(name, "Authorization") {
			req.Header.Set("Authorization", value)
		}
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.New("hash endpoint request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hash endpoint returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxHashResponseBytes+1))
	if err != nil || len(data) > maxHashResponseBytes {
		return nil, errors.New("hash endpoint response exceeds limit or could not be read")
	}
	var response nativeHashResponse
	if json.Unmarshal(data, &response) != nil {
		return nil, errors.New("invalid hash endpoint response")
	}
	if err := response.validate(); err != nil {
		return nil, err
	}
	return &response, nil
}

func (r *nativeHashResponse) validate() error {
	if r.SchemaVersion != 1 || r.Model == "" || r.HashAlgorithm == "" || r.HashEncoding != "hex" ||
		r.HashBlockSize <= 0 || r.Lookahead < 0 || len(r.Groups) == 0 || len(r.Prompts) == 0 {
		return errors.New("invalid hash endpoint metadata")
	}
	groups := sets.New[int]()
	for _, group := range r.Groups {
		if group.GroupIdx < 0 || group.BlockSize <= 0 || group.BlockSize%r.HashBlockSize != 0 || groups.Has(group.GroupIdx) {
			return errors.New("invalid hash endpoint cache group")
		}
		groups.Insert(group.GroupIdx)
	}
	for i := range r.Prompts {
		prompt := &r.Prompts[i]
		if prompt.NumTokens <= 0 || len(prompt.BlockHashes) != max(0, prompt.NumTokens-r.Lookahead)/r.HashBlockSize {
			return errors.New("invalid hash endpoint prompt coverage")
		}
		prompt.hashes = make([]uint64, len(prompt.BlockHashes))
		for j, encoded := range prompt.BlockHashes {
			decoded, err := hex.DecodeString(encoded)
			if err != nil || len(decoded) < 8 || len(decoded) > 64 {
				return errors.New("invalid hash endpoint block hash")
			}
			// Matches both the byte and legacy uint64 KV-event encodings.
			prompt.hashes[j] = binary.BigEndian.Uint64(decoded[len(decoded)-8:])
		}
	}
	return nil
}

func (p *Producer) produceNativeHashes(ctx context.Context, request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) error {
	if request == nil || request.Body == nil || request.Body.RenderRequest || len(endpoints) == 0 {
		return nil
	}
	hashes, err := p.nativeHasher.hash(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.FromContext(ctx).V(logging.DEBUG).Info("Skipping native cache prediction", "reason", err.Error())
		return nil
	}
	if hashes == nil {
		return nil
	}
	filter := sets.New[string]()
	for _, endpoint := range endpoints {
		if md := endpoint.GetMetadata(); md != nil {
			filter.Insert(fmt.Sprintf("%s:%s", md.Address, md.Port))
		}
	}
	if len(filter) == 0 {
		return nil
	}
	allMatches := make(map[string]kvcache.PodMatch)
	totalTokens := 0
	for _, prompt := range hashes.Prompts {
		totalTokens += prompt.NumTokens
		var matches map[string]kvcache.PodMatch
		firstGroup := true
		for _, group := range hashes.Groups {
			switch group.Kind {
			case kvevents.KVCacheSpecKindFullAttention, kvevents.KVCacheSpecKindMlaAttention, kvevents.KVCacheSpecKindSinkFull:
			default:
				continue
			}
			stride := group.BlockSize / hashes.HashBlockSize
			keys := make([]kvblock.BlockHash, 0, len(prompt.hashes)/stride)
			for pos := stride - 1; pos < len(prompt.hashes); pos += stride {
				keys = append(keys, kvblock.NativeBlockKey(hashes.Model, &group.GroupIdx, prompt.hashes[pos]))
			}
			groupMatches, err := p.kvCacheIndexer.MatchBlockKeys(ctx, keys, filter)
			if err != nil {
				return err
			}
			for pod, match := range groupMatches {
				match.MatchedBlocks *= group.BlockSize
				match.ConfirmedBlocks *= group.BlockSize
				match.WeightedScore *= float64(group.BlockSize)
				for tier, count := range match.BlocksByTier {
					match.BlocksByTier[tier] = count * group.BlockSize
				}
				groupMatches[pod] = match
			}
			if firstGroup {
				matches = groupMatches
				firstGroup = false
			} else {
				for pod, match := range matches {
					other := groupMatches[pod]
					match.MatchedBlocks = min(match.MatchedBlocks, other.MatchedBlocks)
					match.ConfirmedBlocks = min(match.ConfirmedBlocks, other.ConfirmedBlocks)
					match.WeightedScore = min(match.WeightedScore, other.WeightedScore)
					for tier, count := range match.BlocksByTier {
						match.BlocksByTier[tier] = min(count, other.BlocksByTier[tier])
					}
					matches[pod] = match
				}
			}
		}
		for pod, match := range matches {
			allMatches[pod] = addPodMatch(allMatches[pod], match)
		}
	}
	results := make([]endpointResult, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if md := endpoint.GetMetadata(); md != nil {
			match := allMatches[fmt.Sprintf("%s:%s", md.Address, md.Port)]
			info := attrprefix.NewPrefixCacheMatchInfo(int(match.WeightedScore), totalTokens, 1).
				WithCachedBlockCount(match.MatchedBlocks).
				WithConfirmedCachedBlockCount(match.ConfirmedBlocks).
				WithCachedBlocksByTier(match.BlocksByTier)
			results = append(results, endpointResult{endpoint: endpoint, info: info})
		}
	}
	return p.publishEndpointResults(ctx, results)
}
