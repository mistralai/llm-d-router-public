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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvevents"
	"github.com/llm-d/llm-d-router/pkg/kvevents/engineadapter"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/common/observability/semconv"
	"github.com/llm-d/llm-d-router/pkg/common/observability/tracing"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	rcplugins "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol"
	tokenproducer "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/tokenizer"
)

// PluginType is the registered type name of the precise-prefix-cache-producer.
const PluginType = "precise-prefix-cache-producer"

const (
	defaultCheckpointInterval = 5 * time.Second
	defaultCheckpointTTL      = 24 * time.Hour
)

// CheckpointConfig configures periodic durable snapshots of the precise index.
type CheckpointConfig struct {
	// StorePluginRef names the plugin that persists checkpoint bytes.
	StorePluginRef string `json:"storePluginRef" pluginRef:""`
	// Key namespaces this producer's checkpoint. The producer name is used
	// when Key is empty.
	Key string `json:"key"`
	// Interval is the checkpoint cadence. The default is 5 seconds.
	Interval string `json:"interval"`
	// TTL is the checkpoint lifetime in the store. The default is 24 hours.
	TTL string `json:"ttl"`
}

// PluginConfig configures the precise-prefix-cache-producer.
type PluginConfig struct {
	TokenProcessorConfig *kvblock.TokenProcessorConfig `json:"tokenProcessorConfig"`
	IndexerConfig        *kvcache.Config               `json:"indexerConfig"`
	KVEventsConfig       *kvevents.Config              `json:"kvEventsConfig"`
	// SpeculativeIndexing seeds predicted cache entries for the selected
	// endpoint(s) immediately after a routing decision, so the next
	// same-prefix request hits without waiting for engine confirmation.
	SpeculativeIndexing bool `json:"speculativeIndexing"`
	// SpeculativeTTL bounds how long speculative entries live before
	// eviction. Go duration string; defaults to defaultSpeculativeTTL when
	// empty.
	SpeculativeTTL string `json:"speculativeTTL"`
	// Checkpoint persists confirmed index and KV-event stream state. The store
	// plugin must implement the checkpointStore methods.
	Checkpoint *CheckpointConfig `json:"checkpoint,omitempty"`
}

type checkpointStore interface {
	SaveCheckpoint(context.Context, string, []byte, time.Duration) error
	LoadCheckpoint(context.Context, string) ([]byte, bool, error)
}

type resolvedCheckpointConfig struct {
	key      string
	interval time.Duration
	ttl      time.Duration
}

var (
	_ requestcontrol.DataProducer = &Producer{}
	_ plugin.StateDumper          = &Producer{}
)

// subscriberManager is the subset of kvevents.SubscriberManager the producer
// relies on, narrowed so tests can substitute a fake.
type subscriberManager interface {
	EnsureSubscriber(
		ctx context.Context,
		podIdentifier, sourceEndpoint, endpoint, replayEndpoint, topicFilter string,
		remoteSocket bool,
	) error
	RemoveSubscriber(ctx context.Context, podIdentifier string) bool
	GetActiveSubscribers() ([]string, []string)
	Shutdown(ctx context.Context)
}

// Producer is a DataProducer plugin that maintains a KV-block prefix-cache
// index by subscribing to engine KV-events and writes per-endpoint
// PrefixCacheMatchInfo for each request. Operators pair it with the
// generic prefix-cache-scorer (set prefixMatchInfoProducerName to this
// producer's instance name) to route requests by precise cache locality.
//
// Speculative-indexing logic lives in prerequest.go; per-pod ZMQ subscriber
// lifecycle in extractor.go.
type Producer struct {
	typedName      plugin.TypedName
	kvCacheIndexer kvCacheIndexer

	subscribersManager subscriberManager
	kvEventsConfig     *kvevents.Config
	podSelector        labels.Selector // nil matches every endpoint.

	dk plugin.DataKey

	pluginState *plugin.PluginState

	speculativeCache   *ttlcache.Cache[string, *speculativeEntries]
	speculativeTTL     time.Duration
	speculativeEnabled bool

	blockSizeTokens int

	kvEventsPool          *kvevents.Pool
	checkpointStore       checkpointStore
	checkpointKey         string
	checkpointInterval    time.Duration
	checkpointTTL         time.Duration
	checkpointFingerprint string

	// Plugin-lifetime, not request-scoped: SubscriberManager binds each
	// subscriber's goroutine to the ctx passed at registration.
	subscriberCtx context.Context
}

// PluginConfigParser parses and validates the producer configuration. The
// registry also uses the returned plugin references to order construction.
func PluginConfigParser(rawParameters *json.Decoder, _ plugin.Handle) (any, error) {
	indexerConfig, err := kvcache.NewDefaultConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize indexer config: %w", err)
	}

	parameters := PluginConfig{
		IndexerConfig:  indexerConfig,
		KVEventsConfig: kvevents.DefaultConfig(),
	}

	if rawParameters != nil {
		if err := rawParameters.Decode(&parameters); err != nil {
			return nil, fmt.Errorf("failed to parse %s plugin config: %w", PluginType, err)
		}
	}

	if parameters.IndexerConfig == nil {
		return nil, errors.New("indexerConfig is required")
	}
	if parameters.Checkpoint != nil {
		if parameters.Checkpoint.StorePluginRef == "" {
			return nil, errors.New("checkpoint.storePluginRef is required")
		}
		if _, err := resolveCheckpointConfig("checkpoint", parameters.Checkpoint); err != nil {
			return nil, err
		}
	}

	return parameters, nil
}

// PluginFactory parses the raw plugin configuration and returns a configured
// Producer.
func PluginFactory(name string, rawParameters *json.Decoder, handle plugin.Handle) (plugin.Plugin, error) {
	rawConfig, err := PluginConfigParser(rawParameters, handle)
	if err != nil {
		return nil, err
	}
	parameters := rawConfig.(PluginConfig)

	var store checkpointStore
	if parameters.Checkpoint != nil {
		storePlugin := handle.Plugin(parameters.Checkpoint.StorePluginRef)
		if storePlugin == nil {
			return nil, fmt.Errorf("checkpoint store plugin not found: %s", parameters.Checkpoint.StorePluginRef)
		}
		var ok bool
		store, ok = storePlugin.(checkpointStore)
		if !ok {
			return nil, fmt.Errorf("plugin %s does not implement precise checkpoint storage", parameters.Checkpoint.StorePluginRef)
		}
	}

	p, err := newProducer(handle.Context(), name, parameters, store)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s plugin: %w", PluginType, err)
	}

	return p, nil
}

// New constructs a precise-prefix-cache-producer. The instance name becomes
// the producer name on PrefixCacheMatchInfoDataKey, which downstream
// consumers must match (see prefix-cache-scorer's prefixMatchInfoProducerName).
// The kvcache indexer, KV-events pool, and any local ZMQ subscriber start
// in background goroutines bound to ctx.
func New(ctx context.Context, name string, config PluginConfig) (*Producer, error) {
	return newProducer(ctx, name, config, nil)
}

func newProducer(ctx context.Context, name string, config PluginConfig, store checkpointStore) (*Producer, error) {
	if config.TokenProcessorConfig == nil {
		config.TokenProcessorConfig = kvblock.DefaultTokenProcessorConfig()
	}
	if config.KVEventsConfig == nil {
		config.KVEventsConfig = kvevents.DefaultConfig()
	}

	var podSelector labels.Selector
	if kc := config.KVEventsConfig; kc.DiscoverPods && kc.PodDiscoveryConfig != nil && kc.PodDiscoveryConfig.PodLabelSelector != "" {
		sel, err := labels.Parse(kc.PodDiscoveryConfig.PodLabelSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid kvEventsConfig.podDiscoveryConfig.podLabelSelector %q: %w",
				kc.PodDiscoveryConfig.PodLabelSelector, err)
		}
		podSelector = sel
	}

	checkpointConfig, checkpointEnabled, err := validateCheckpointConfig(name, config, store)
	if err != nil {
		return nil, err
	}

	tokenProcessor, err := kvblock.NewChunkedTokenDatabase(config.TokenProcessorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create token processor: %w", err)
	}

	indexer, err := kvcache.NewKVCacheIndexer(ctx, config.IndexerConfig, tokenProcessor)
	if err != nil {
		return nil, fmt.Errorf("failed to create kvcache.Indexer: %w", err)
	}

	adapter, err := engineadapter.NewAdapter(config.KVEventsConfig.EngineType)
	if err != nil {
		return nil, fmt.Errorf("failed to create KV-events engine adapter: %w", err)
	}
	pool, err := kvevents.NewPool(config.KVEventsConfig, indexer.KVBlockIndex(), tokenProcessor, adapter)
	if err != nil {
		return nil, fmt.Errorf("failed to create KV-events pool: %w", err)
	}

	fingerprint := ""
	if checkpointEnabled {
		if err := pool.EnableCheckpointing(); err != nil {
			return nil, fmt.Errorf("enable precise index checkpointing: %w", err)
		}
		fingerprint = checkpointFingerprint(
			config.TokenProcessorConfig,
			tokenProcessor.BlockSize(),
			config.IndexerConfig.KVBlockIndexConfig.InMemoryConfig,
		)
		checkpointConfig.key += ":" + fingerprint
		started := time.Now()
		data, found, err := store.LoadCheckpoint(ctx, checkpointConfig.key)
		if err != nil {
			return nil, fmt.Errorf("load precise index checkpoint: %w", err)
		}
		if found {
			if err := pool.RestoreCheckpoint(data, fingerprint); err != nil {
				return nil, fmt.Errorf("restore precise index checkpoint: %w", err)
			}
			log.FromContext(ctx).WithName(PluginType).Info("Restored precise index checkpoint",
				"key", checkpointConfig.key, "bytes", len(data), "duration", time.Since(started))
		}
	}

	go indexer.Run(ctx)
	pool.Start(ctx)

	subscribersManager := kvevents.NewSubscriberManager(pool)
	if config.KVEventsConfig.ZMQEndpoint != "" {
		if err := subscribersManager.EnsureSubscriber(ctx, "local-subscriber", "",
			config.KVEventsConfig.ZMQEndpoint, "", config.KVEventsConfig.TopicFilter, false); err != nil {
			return nil, fmt.Errorf("failed to create local subscriber for global socket mode: %w", err)
		}
	}

	speculativeCache, speculativeTTL, err := buildSpeculativeCache(ctx, config, indexer.KVBlockIndex())
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		typedName:          plugin.TypedName{Type: PluginType, Name: name},
		kvCacheIndexer:     indexer,
		subscribersManager: subscribersManager,
		kvEventsConfig:     config.KVEventsConfig,
		podSelector:        podSelector,
		dk:                 attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(name),
		pluginState:        plugin.NewPluginState(ctx),
		speculativeCache:   speculativeCache,
		speculativeTTL:     speculativeTTL,
		speculativeEnabled: config.SpeculativeIndexing,
		blockSizeTokens:    tokenProcessor.BlockSize(),
		kvEventsPool:       pool,
		subscriberCtx:      ctx,
	}
	if checkpointEnabled {
		producer.checkpointStore = store
		producer.checkpointKey = checkpointConfig.key
		producer.checkpointInterval = checkpointConfig.interval
		producer.checkpointTTL = checkpointConfig.ttl
		producer.checkpointFingerprint = fingerprint
		go producer.runCheckpointLoop(ctx)
	}
	return producer, nil
}

func validateCheckpointConfig(
	name string,
	config PluginConfig,
	store checkpointStore,
) (resolvedCheckpointConfig, bool, error) {
	if config.Checkpoint == nil {
		return resolvedCheckpointConfig{}, false, nil
	}
	if store == nil {
		return resolvedCheckpointConfig{}, false, errors.New("checkpoint store is required")
	}
	if !config.KVEventsConfig.DiscoverPods || config.KVEventsConfig.PodDiscoveryConfig == nil {
		return resolvedCheckpointConfig{}, false, errors.New("checkpointing requires per-pod KV-event discovery")
	}
	if config.KVEventsConfig.ZMQEndpoint != "" {
		return resolvedCheckpointConfig{}, false, errors.New("checkpointing does not support a global KV-event socket")
	}
	if config.KVEventsConfig.PodDiscoveryConfig.EffectiveReplayPort() <= 0 {
		return resolvedCheckpointConfig{}, false, errors.New("checkpointing requires kvEventsConfig.podDiscoveryConfig.replaySocketPort")
	}
	if config.IndexerConfig == nil || config.IndexerConfig.KVBlockIndexConfig == nil {
		return resolvedCheckpointConfig{}, false, errors.New("checkpointing requires an in-memory index")
	}
	indexConfig := config.IndexerConfig.KVBlockIndexConfig
	if indexConfig.InMemoryConfig == nil || indexConfig.RedisConfig != nil || indexConfig.CostAwareMemoryConfig != nil {
		return resolvedCheckpointConfig{}, false, errors.New("checkpointing requires an in-memory index")
	}
	resolved, err := resolveCheckpointConfig(name, config.Checkpoint)
	if err != nil {
		return resolvedCheckpointConfig{}, false, err
	}
	return resolved, true, nil
}

func resolveCheckpointConfig(name string, config *CheckpointConfig) (resolvedCheckpointConfig, error) {
	interval, err := parsePositiveDuration("checkpoint.interval", config.Interval, defaultCheckpointInterval)
	if err != nil {
		return resolvedCheckpointConfig{}, err
	}
	ttl, err := parsePositiveDuration("checkpoint.ttl", config.TTL, defaultCheckpointTTL)
	if err != nil {
		return resolvedCheckpointConfig{}, err
	}
	key := config.Key
	if key == "" {
		key = name
	}
	return resolvedCheckpointConfig{key: key, interval: interval, ttl: ttl}, nil
}

func parsePositiveDuration(field, value string, defaultValue time.Duration) (time.Duration, error) {
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", field, value, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", field, parsed)
	}
	return parsed, nil
}

func checkpointFingerprint(
	tokenConfig *kvblock.TokenProcessorConfig,
	blockSizeTokens int,
	indexConfig *kvblock.InMemoryIndexConfig,
) string {
	hashAlgorithm := tokenConfig.HashAlgorithm
	if hashAlgorithm == "" {
		hashAlgorithm = kvblock.HashAlgorithmCBORFNV
	}
	podCacheSize := indexConfig.PodCacheSize
	if podCacheSize <= 0 {
		podCacheSize = kvblock.DefaultInMemoryIndexConfig().PodCacheSize
	}
	material, _ := json.Marshal(struct {
		SchemaVersion   int    `json:"schemaVersion"`
		BlockSizeTokens int    `json:"blockSizeTokens"`
		HashSeed        string `json:"hashSeed"`
		HashAlgorithm   string `json:"hashAlgorithm"`
		IndexSize       int    `json:"indexSize"`
		PodCacheSize    int    `json:"podCacheSize"`
	}{
		SchemaVersion:   1,
		BlockSizeTokens: blockSizeTokens,
		HashSeed:        tokenConfig.HashSeed,
		HashAlgorithm:   hashAlgorithm,
		IndexSize:       indexConfig.Size,
		PodCacheSize:    podCacheSize,
	})
	digest := sha256.Sum256(material)
	return hex.EncodeToString(digest[:])
}

func (p *Producer) runCheckpointLoop(ctx context.Context) {
	ticker := time.NewTicker(p.checkpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.writeCheckpoint(ctx); err != nil {
				if errors.Is(err, kvevents.ErrCheckpointReplayInProgress) {
					log.FromContext(ctx).V(logging.DEBUG).Info("Skipping precise index checkpoint while replay is in progress")
					continue
				}
				log.FromContext(ctx).Error(err, "Failed to save precise index checkpoint")
			}
		}
	}
}

func (p *Producer) writeCheckpoint(ctx context.Context) error {
	started := time.Now()
	data, result, err := p.kvEventsPool.MarshalCheckpoint(p.checkpointFingerprint)
	if err != nil {
		return err
	}
	if err := p.checkpointStore.SaveCheckpoint(ctx, p.checkpointKey, data, p.checkpointTTL); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	log.FromContext(ctx).V(logging.DEBUG).Info("Saved precise index checkpoint",
		"key", p.checkpointKey, "bytes", result.Bytes, "sources", result.Sources,
		"createdAt", result.CreatedAt, "duration", time.Since(started))
	return nil
}

// TypedName returns the plugin's registered type and name.
func (p *Producer) TypedName() plugin.TypedName {
	return p.typedName
}

// Debug-dump caps keep the payload bounded. A list is partial when its matching
// TotalX exceeds MaxX.
const (
	maxDumpSubscribers        = 100
	maxDumpSpeculativeEntries = 100
)

// precisePrefixState is the snapshot returned by DumpState. The KV-block index
// is keyed by prompt-derived block hashes and is not enumerable, so it is not
// reported; the active subscriber pod identities and the live speculative
// request ids are enumerated (sorted and capped) for debugging.
type precisePrefixState struct {
	Subscribers             []string `json:"subscribers"`
	TotalSubscribers        int      `json:"totalSubscribers"`
	MaxSubscribers          int      `json:"maxSubscribers"`
	SpeculativeIndexing     bool     `json:"speculativeIndexing"`
	SpeculativeEntries      []string `json:"speculativeEntries"`
	TotalSpeculativeEntries int      `json:"totalSpeculativeEntries"`
	MaxSpeculativeEntries   int      `json:"maxSpeculativeEntries"`
	BlockSizeTokens         int      `json:"blockSizeTokens"`
}

// DumpState reports the producer's bounded operational state: the active
// KV-event subscriber pod identities, whether speculative indexing is on and
// the live speculative request ids, and the block size in tokens. Both lists
// are sorted and capped; the prompt-derived block index is not exposed.
func (p *Producer) DumpState() (json.RawMessage, error) {
	subscribers := []string{}
	var totalSubscribers int
	if p.subscribersManager != nil {
		ids, _ := p.subscribersManager.GetActiveSubscribers()
		totalSubscribers = len(ids)
		subscribers = sortedCapped(ids, maxDumpSubscribers)
	}
	speculativeEntries := []string{}
	var totalSpeculativeEntries int
	if p.speculativeCache != nil {
		keys := p.speculativeCache.Keys()
		totalSpeculativeEntries = len(keys)
		speculativeEntries = sortedCapped(keys, maxDumpSpeculativeEntries)
	}
	return json.Marshal(precisePrefixState{
		Subscribers:             subscribers,
		TotalSubscribers:        totalSubscribers,
		MaxSubscribers:          maxDumpSubscribers,
		SpeculativeIndexing:     p.speculativeEnabled,
		SpeculativeEntries:      speculativeEntries,
		TotalSpeculativeEntries: totalSpeculativeEntries,
		MaxSpeculativeEntries:   maxDumpSpeculativeEntries,
		BlockSizeTokens:         p.blockSizeTokens,
	})
}

// sortedCapped returns a sorted copy of in (never nil, so empty lists serialize
// as [] not null), truncated to limit entries.
func sortedCapped(in []string, limit int) []string {
	out := append([]string{}, in...)
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Produces declares the PrefixCacheMatchInfoDataKey published per endpoint,
// name-bound to this producer instance.
func (p *Producer) Produces() map[plugin.DataKey]any {
	return map[plugin.DataKey]any{p.dk: attrprefix.PrefixCacheMatchInfo{}}
}

// Consumes declares the TokenizedRequest dependency from token-producer so
// the data-layer DAG orders tokenization before this producer runs.
func (p *Producer) Consumes() plugin.DataDependencies {
	return plugin.DataDependencies{
		Required: map[plugin.DataKey]any{tokenproducer.TokenizedPromptDataKey: scheduling.TokenizedRequest{}},
	}
}

// Produce hashes the request's TokenizedRequest into KV-block keys, looks
// them up in the per-endpoint KV-block index, and writes PrefixCacheMatchInfo
// to each candidate endpoint. No-op when the request carries no tokens.
// With speculativeIndexing enabled, the computed block keys are stashed
// for PreRequest to seed the index after a routing decision is made.
func (p *Producer) Produce(ctx context.Context,
	request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint,
) error {
	ctx, span := tracing.Tracer(rcplugins.TracerScope).Start(ctx, "produce_precise_prefix_cache",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	span.SetAttributes(semconv.LLMDEPPProducerCandidateEndpoints(len(endpoints)))
	if request != nil {
		if request.TargetModel != "" {
			span.SetAttributes(semconv.GenAIRequestModel(request.TargetModel))
		}
		if request.RequestID != "" {
			span.SetAttributes(semconv.GenAIRequestID(request.RequestID))
		}
	}

	perPromptKeys, mmBlockIndices, err := computeBlockKeys(ctx, p.kvCacheIndexer, request, p.blockSizeTokens)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to compute block keys: %w", err)
	}
	if len(perPromptKeys) == 0 {
		span.SetAttributes(semconv.LLMDEPPProducerResult("skipped_no_tokens"))
		return nil
	}

	return p.produceFromBlockKeys(ctx, span, request, endpoints, perPromptKeys, mmBlockIndices)
}

func (p *Producer) produceFromBlockKeys(ctx context.Context, span trace.Span,
	request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint,
	perPromptKeys [][]kvblock.BlockHash, mmBlockIndices []int,
) error {
	logger := log.FromContext(ctx).WithName(p.typedName.String())
	endpointSet := extractEndpointSet(endpoints)

	// A multi-prompt request scores as the sum of its prompts' matches. The
	// first prompt's result is the aggregate, so single-prompt requests copy
	// nothing.
	var matches map[string]kvcache.PodMatch
	totalBlocks := 0
	for _, blockKeys := range perPromptKeys {
		promptMatches, err := p.kvCacheIndexer.MatchBlockKeys(ctx, blockKeys, endpointSet)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("failed to match block keys: %w", err)
		}
		totalBlocks += len(blockKeys)
		if matches == nil {
			matches = promptMatches
			continue
		}
		for pod, m := range promptMatches {
			matches[pod] = addPodMatch(matches[pod], m)
		}
	}

	maxMatch := 0
	results := make([]endpointResult, 0, len(endpoints))
	for _, ep := range endpoints {
		if err := ctx.Err(); err != nil {
			return err
		}
		md := ep.GetMetadata()
		if md == nil {
			continue
		}
		match := matches[fmt.Sprintf("%s:%s", md.Address, md.Port)]
		if match.BlocksByTier == nil {
			match.BlocksByTier = map[string]int{} // no match: consumers still read a map
		}
		matchLen := int(match.WeightedScore)
		if matchLen > maxMatch {
			maxMatch = matchLen
		}
		info := attrprefix.NewPrefixCacheMatchInfo(matchLen, totalBlocks, p.blockSizeTokens).
			WithCachedBlockCount(match.MatchedBlocks).
			WithConfirmedCachedBlockCount(match.ConfirmedBlocks).
			WithCachedBlocksByTier(match.BlocksByTier)
		if len(mmBlockIndices) > 0 {
			info.WithMM(attrprefix.MMMatchInfo{MatchBlocks: countMMMatchedBlocks(mmBlockIndices, match.MatchedBlocks)})
		}
		results = append(results, endpointResult{endpoint: ep, info: info})
	}
	if err := p.publishEndpointResults(ctx, results); err != nil {
		return err
	}

	if p.speculativeEnabled {
		p.pluginState.Write(request.RequestID, blockKeysStateKey,
			&blockKeysState{perPromptKeys: perPromptKeys})
	}

	span.SetAttributes(
		semconv.LLMDEPPProducerTotalBlocks(totalBlocks),
		semconv.LLMDEPPProducerMaxMatchBlocks(maxMatch),
	)

	if v := logger.V(logging.TRACE); v.Enabled() {
		v.Info("Produce completed", "blockKeys", totalBlocks, "matches", matches)
	}
	return nil
}

// addPodMatch sums b into a. A zero a (a pod first seen in a later prompt)
// takes b as is.
func addPodMatch(a, b kvcache.PodMatch) kvcache.PodMatch {
	if a.BlocksByTier == nil {
		return b
	}
	a.WeightedScore += b.WeightedScore
	a.MatchedBlocks += b.MatchedBlocks
	a.ConfirmedBlocks += b.ConfirmedBlocks
	for tier, count := range b.BlocksByTier {
		a.BlocksByTier[tier] += count
	}
	return a
}

type endpointResult struct {
	endpoint scheduling.Endpoint
	info     *attrprefix.PrefixCacheMatchInfo
}

func (p *Producer) publishEndpointResults(ctx context.Context, results []endpointResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, result := range results {
		result.endpoint.Put(p.dk, result.info)
	}
	return nil
}
