// Copyright 2025 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvevents

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/common/observability/semconv"
	"github.com/llm-d/llm-d-router/pkg/common/observability/tracing"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvcache/metrics"
)

const (
	defaultEventSourceDeviceTier = "gpu"
	defaultPodSelector           = ""
	resetRetryInterval           = 100 * time.Millisecond
	// DefaultCheckpointQueueDepth bounds ingress while checkpoint I/O stops workers.
	DefaultCheckpointQueueDepth = 65536
)

// normalizeDeviceTier lowercases an event's device tier and defaults an empty
// value to the GPU source tier. Store and remove events that mean the same tier
// must normalize identically so they build equal PodEntries and dedup scopes;
// keeping this in one place prevents the two call sites from drifting apart.
func normalizeDeviceTier(deviceTier string) string {
	if deviceTier == "" {
		return defaultEventSourceDeviceTier
	}
	return strings.ToLower(deviceTier)
}

func isPrefixIndexableSpecKind(kind KVCacheSpecKind) bool {
	switch kind {
	case KVCacheSpecKindFullAttention, KVCacheSpecKindMlaAttention, KVCacheSpecKindSinkFull:
		return true
	default:
		return false
	}
}

func cacheKindLabel(kind KVCacheSpecKind) string {
	if kind == "" {
		return string(KVCacheSpecKindUnknown)
	}
	return string(kind)
}

func blockStoredEventDigestible(ev *BlockStoredEvent) (bool, string) {
	if ev.GroupIdx == nil {
		return true, ""
	}
	if *ev.GroupIdx < 0 {
		return false, "invalid_group"
	}
	if !isPrefixIndexableSpecKind(ev.KVCacheSpecKind) {
		return false, "unsupported_cache_kind"
	}
	if ev.BlockSize <= 0 {
		return false, "invalid_block_size"
	}
	if ev.KVCacheSpecSlidingWindowSize != nil && *ev.KVCacheSpecSlidingWindowSize < 0 {
		return false, "invalid_sliding_window"
	}
	if len(ev.Tokens) == 0 {
		return true, ""
	}
	if len(ev.Tokens)%ev.BlockSize != 0 || len(ev.Tokens)/ev.BlockSize != len(ev.BlockHashes) {
		return false, "non_dense_block_span"
	}
	return true, ""
}

func blockStoredEventGroupMetadata(ev *BlockStoredEvent) (kvblock.GroupMetadata, bool) {
	if ev.GroupIdx == nil || *ev.GroupIdx < 0 || ev.KVCacheSpecKind == "" || ev.BlockSize <= 0 {
		return kvblock.GroupMetadata{}, false
	}
	if ev.KVCacheSpecSlidingWindowSize != nil && *ev.KVCacheSpecSlidingWindowSize < 0 {
		return kvblock.GroupMetadata{}, false
	}
	return kvblock.GroupMetadata{
		Kind:              string(ev.KVCacheSpecKind),
		BlockSize:         ev.BlockSize,
		SlidingWindowSize: ev.KVCacheSpecSlidingWindowSize,
	}, true
}

// Config holds the configuration for the event processing pool.
type Config struct {
	// ZMQEndpoint is the ZMQ address to connect to (e.g., "tcp://indexer:5557").
	ZMQEndpoint string `json:"zmqEndpoint,omitempty"`
	// TopicFilter is the ZMQ subscription filter (e.g., "kv@").
	TopicFilter string `json:"topicFilter"`
	// Concurrency is the number of parallel workers to run.
	Concurrency int `json:"concurrency"`
	// MaxQueueDepth limits messages that wait for event processing.
	// A full queue applies backpressure to subscribers. Zero disables the limit.
	MaxQueueDepth int `json:"maxQueueDepth,omitempty"`
	// EngineType selects the inference engine adapter ("vllm" or "sglang").
	// Default: "vllm".
	EngineType string `json:"engineType,omitempty"`
	// Tracing enables the receive, process and decode spans this package emits.
	// KV events arrive at many times the inference request rate, so those spans
	// are opt-in: with a shared head sampler, always-on event spans would crowd
	// request traces out of the exported volume. Index spans reached from the
	// event path are emitted by the traced Index wrapper and are not gated
	// here, so an exporter still receives those with this unset.
	Tracing bool `json:"tracing,omitempty"`
	// DiscoverPods enables the Kubernetes pod reconciler for automatic
	// per-pod subscriber management. When enabled, the reconciler watches
	// Kubernetes pods and creates/removes ZMQ subscribers dynamically.
	DiscoverPods bool `json:"discoverPods"`
	// PodDiscoveryConfig holds the configuration for pod discovery.
	// Only used when DiscoverPods is true.
	PodDiscoveryConfig *PodDiscoveryConfig `json:"podDiscoveryConfig,omitempty"`
}

// PodDiscoveryConfig holds configuration for the Kubernetes pod reconciler.
type PodDiscoveryConfig struct {
	// PodLabelSelector is a label selector string for filtering which pods to watch.
	// Empty matches every pod.
	// Example: "app=vllm" or "app=vllm,tier=gpu"
	PodLabelSelector string `json:"podLabelSelector"`
	// PodNamespace limits the reconciler to watch pods in a specific namespace.
	// If empty, watches all namespaces (requires appropriate RBAC).
	PodNamespace string `json:"podNamespace,omitempty"`
	// SocketPort is the port number where vLLM pods expose their ZMQ socket.
	// The reconciler will connect to tcp://<PodIP>:<SocketPort>
	// Default: 5557
	SocketPort int `json:"socketPort"`
	// ReplaySocketPort is the port where vLLM pods expose their ZMQ ROUTER
	// socket for replay requests. Disabled when not set (0 or negative).
	ReplaySocketPort int `json:"replaySocketPort,omitempty"`
	// DataParallelSize is accepted for compatibility with configurations that
	// predate rank metadata discovery. It is ignored.
	DataParallelSize int `json:"dataParallelSize,omitempty"`
	// RankPodMapping maps logical ranks behind one serving endpoint to separate
	// Kubernetes pods that host their KV-event sockets.
	RankPodMapping *RankPodMappingConfig `json:"rankPodMapping,omitempty"`
}

// RankPodMappingConfig describes a multi-pod data-parallel group. Pods are
// grouped with GroupLabelKey. RankLabelKey identifies each pod's zero-based
// worker index.
type RankPodMappingConfig struct {
	GroupLabelKey string `json:"groupLabelKey"`
	RankLabelKey  string `json:"rankLabelKey"`
	// RanksPerPod is the number of consecutive global DP ranks hosted by each
	// worker pod. Values of zero default to one.
	RanksPerPod int `json:"ranksPerPod,omitempty"`
}

// EffectiveReplayPort returns the replay socket port.
// Returns -1 (disabled) when not explicitly configured.
func (c *PodDiscoveryConfig) EffectiveReplayPort() int {
	if c.ReplaySocketPort <= 0 {
		return -1
	}
	return c.ReplaySocketPort
}

// DefaultPodReconcilerConfig returns a default configuration for the pod reconciler.
func DefaultPodReconcilerConfig() *PodDiscoveryConfig {
	return &PodDiscoveryConfig{
		PodLabelSelector: defaultPodSelector,
		SocketPort:       5557,
	}
}

// DefaultConfig returns a default configuration for the event processing pool.
func DefaultConfig() *Config {
	return &Config{
		TopicFilter:        "kv@",
		Concurrency:        4,
		DiscoverPods:       true,
		PodDiscoveryConfig: DefaultPodReconcilerConfig(),
	}
}

// Pool is a sharded worker pool that processes events from ZMQ subscribers.
// It ensures that events for the same PodIdentifier are processed in order.
// Pool keeps transient event-stream state while durable key mappings are
// delegated to the Index.
type Pool struct {
	queues         []workqueue.TypedRateLimitingInterface[*RawMessage]
	concurrency    int // can replace use with len(queues)
	index          kvblock.Index
	tokenProcessor kvblock.TokenProcessor
	adapter        EngineAdapter
	groupCatalog   *kvblock.GroupCatalog
	// dedup lives in the Pool, not as an Index decorator, because its scope is
	// built from event fields absent from the Index.Evict signature (device
	// tier, KV-cache group, DP rank) and a store must be counted only after
	// Index.Add succeeds — both of which only the Pool observes.
	dedup *eventDedupFilter
	// checkpointMu stops event application at a complete message boundary.
	checkpointMu       sync.RWMutex
	checkpointWriteMu  sync.Mutex
	appliedSourcesMu   sync.RWMutex
	appliedSources     map[string]checkpointSource
	invalidSources     map[string]map[uint64]struct{}
	sourceGenerations  map[string]uint64
	retiredGenerations map[string]map[uint64]struct{}
	nextGeneration     atomic.Uint64
	nextInvalidation   atomic.Uint64
	// tracer is resolved once: tracing.Tracer rebuilds its instrumentation
	// options on every call, which is not free on the per-message event path.
	// Nil when Config.Tracing is unset, which is what startSpan tests to skip
	// span construction on the default path.
	tracer trace.Tracer
	wg     sync.WaitGroup
	// queueDepth mirrors the number of tasks queued across all shards. It is
	// tracked incrementally rather than by summing queue.Len() so that the
	// depth gauge stays O(1) on the enqueue/dequeue hot path.
	queueDepth atomic.Int64
	started    atomic.Bool
	queueSlots chan struct{}
	stopped    chan struct{}
	stopOnce   sync.Once
}

// NewPool creates a Pool with a sharded worker setup.
// Subscribers are managed by SubscriberManager which is controlled by the pod
// reconciler.
//
// Side effect: it registers the kvcache metrics with the controller-runtime
// registry so that the kvevents metrics are scraped wherever a pool runs.
// Registration is idempotent (guarded by a sync.Once).
func NewPool(cfg *Config, index kvblock.Index, tokenProcessor kvblock.TokenProcessor,
	adapter EngineAdapter,
) (*Pool, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if cfg.Concurrency <= 0 {
		return nil, fmt.Errorf("kvEventsConfig.concurrency must be positive, got %d", cfg.Concurrency)
	}
	if cfg.MaxQueueDepth < 0 {
		return nil, fmt.Errorf("kvEventsConfig.maxQueueDepth must not be negative, got %d", cfg.MaxQueueDepth)
	}
	p := &Pool{
		queues:             make([]workqueue.TypedRateLimitingInterface[*RawMessage], cfg.Concurrency),
		concurrency:        cfg.Concurrency,
		index:              index,
		tokenProcessor:     tokenProcessor,
		adapter:            adapter,
		groupCatalog:       kvblock.NewGroupCatalog(),
		dedup:              newEventDedupFilter(),
		appliedSources:     make(map[string]checkpointSource),
		invalidSources:     make(map[string]map[uint64]struct{}),
		sourceGenerations:  make(map[string]uint64),
		retiredGenerations: make(map[string]map[uint64]struct{}),
		tracer:             newEventTracer(cfg.Tracing),
		stopped:            make(chan struct{}),
	}
	if cfg.MaxQueueDepth > 0 {
		p.queueSlots = make(chan struct{}, cfg.MaxQueueDepth)
	}

	for i := 0; i < p.concurrency; i++ {
		p.queues[i] = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[*RawMessage]())
	}

	metrics.Register()

	return p, nil
}

// Span start options are built once. Passing them variadically at each call
// site allocates a fresh slice per message.
var (
	consumerSpanOptions = []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer)}
	internalSpanOptions = []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindInternal)}
)

// disabledSpan stands in for a real span when event tracing is off. A single
// package-level value keeps the default path free of allocation while call
// sites use the span unconditionally.
var disabledSpan trace.Span = tracenoop.Span{}

// newEventTracer returns the event-pipeline tracer, or nil when event tracing
// is disabled. Resolving it once keeps the per-message path clear of
// tracing.Tracer's per-call option building.
func newEventTracer(enabled bool) trace.Tracer {
	if !enabled {
		return nil
	}
	return tracing.Tracer(TracerScope)
}

// startSpan opens a pipeline span, or leaves ctx untouched and returns a span
// that records nothing when event tracing is disabled. The default path skips
// Start rather than relying on a no-op tracer, which still builds an option
// slice and a context per call. See Config.Tracing.
func (p *Pool) startSpan(ctx context.Context, name string, opts []trace.SpanStartOption) (context.Context, trace.Span) {
	if p.tracer == nil {
		return ctx, disabledSpan
	}
	return p.tracer.Start(ctx, name, opts...)
}

// addQueueDepth adjusts the tracked queue depth by delta and publishes the new
// total to the depth gauge.
func (p *Pool) addQueueDepth(delta int64) {
	metrics.PoolQueueDepth.Set(float64(p.queueDepth.Add(delta)))
}

// QueueDepth returns the number of messages that wait for a worker.
func (p *Pool) QueueDepth() int64 {
	return p.queueDepth.Load()
}

// GroupCatalog returns the KV cache group metadata learned from events.
func (p *Pool) GroupCatalog() *kvblock.GroupCatalog {
	return p.groupCatalog
}

// Start begins the worker pool.
// It is non-blocking.
func (p *Pool) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.Info("Starting sharded event processing pool", "workers", p.concurrency)

	metrics.PoolCapacity.Set(float64(p.concurrency))
	p.started.Store(true)

	p.wg.Add(p.concurrency)
	for i := 0; i < p.concurrency; i++ {
		// Each worker is given its own dedicated queue shard.
		go p.worker(ctx, i)
	}
}

// Shutdown gracefully stops the pool and its global subscriber if present.
func (p *Pool) Shutdown(ctx context.Context) {
	logger := log.FromContext(ctx)
	logger.Info("Shutting down event processing pool...")

	p.stopOnce.Do(func() { close(p.stopped) })
	for _, queue := range p.queues {
		queue.ShutDown()
	}

	p.wg.Wait()

	// Tasks still queued at shutdown are dropped with the queues, so reset the
	// depth rather than leaving the gauge pinned at the undrained count.
	p.queueDepth.Store(0)
	metrics.PoolQueueDepth.Set(0)

	logger.Info("event processing pool shut down.")
}

// AddTask is called by the subscriber to add a message to the processing queue.
// It hashes the sharding key to select a queue, ensuring messages for the
// same source endpoint always go to the same worker (ordered queue).
func (p *Pool) AddTask(task *RawMessage) {
	key := task.SourceEndpoint
	if key == "" {
		key = task.EventSourceID
	}
	if key == "" {
		key = p.adapter.ShardingKey(task)
	}
	// Use an FNV-1a hash to deterministically select a queue.
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return
	}

	//nolint:gosec // if concurrency overflows then the world is in trouble anyway
	queueIndex := h.Sum32() % uint32(p.concurrency)
	if p.queueSlots != nil {
		select {
		case p.queueSlots <- struct{}{}:
		case <-p.stopped:
			return
		}
	}
	if p.queues[queueIndex].ShuttingDown() {
		if p.queueSlots != nil {
			<-p.queueSlots
		}
		return
	}
	p.queues[queueIndex].Add(task)
	p.addQueueDepth(1)
}

// resetForSource queues an engine reset on the same shard as its event stream.
func (p *Pool) resetForSource(
	topic, eventSourceID, eventEndpoint, sourceEndpoint string,
	dataParallelRank *int,
) {
	resetVersion := p.beginSourceReset(eventSourceID)
	p.resetSourceInvalidations(
		topic, eventSourceID, eventEndpoint, sourceEndpoint, dataParallelRank, []uint64{resetVersion},
	)
}

func (p *Pool) resetSourceInvalidations(
	topic, eventSourceID, eventEndpoint, sourceEndpoint string,
	dataParallelRank *int,
	resetVersions []uint64,
) {
	p.AddTask(&RawMessage{
		Topic:                 topic,
		EventSourceID:         eventSourceID,
		EventEndpoint:         eventEndpoint,
		SourceEndpoint:        sourceEndpoint,
		ResetDataParallelRank: dataParallelRank,
		reset:                 true,
		resetVersions:         resetVersions,
	})
}

// worker is the main processing loop for a single worker goroutine.
// It processes messages from its dedicated queue using the workqueue pattern.
func (p *Pool) worker(ctx context.Context, workerIndex int) {
	defer p.wg.Done()
	queue := p.queues[workerIndex]
	for {
		task, shutdown := queue.Get()
		if shutdown {
			return
		}
		if p.queueSlots != nil {
			<-p.queueSlots
		}
		p.addQueueDepth(-1)

		// Use a nested func to ensure Done is always called.
		func(task *RawMessage) {
			defer queue.Done(task)
			for !p.processRawMessage(ctx, task) {
				if !task.reset {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-p.stopped:
					return
				case <-time.After(resetRetryInterval):
				}
			}
			// Remove the completed task from workqueue retry tracking.
			queue.Forget(task)
		}(task)
		// Check if context was cancelled after processing a task.
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// processRawMessage decodes the raw message payload using the adapter and processes the resulting event batch.
func (p *Pool) processRawMessage(ctx context.Context, msg *RawMessage) bool {
	p.checkpointMu.RLock()
	defer p.checkpointMu.RUnlock()
	if !msg.reset && p.sourceGenerationIsRetired(msg.EventSourceID, msg.sourceGeneration) {
		return true
	}

	logger := log.FromContext(ctx)
	if msg.reset {
		podID := msg.SourceEndpoint
		if podID == "" {
			podID = p.adapter.ShardingKey(msg)
		}
		var err error
		if msg.ResetDataParallelRank == nil {
			err = p.clearPod(ctx, podID)
		} else {
			err = p.clearRank(ctx, podID, *msg.ResetDataParallelRank)
		}
		if err != nil {
			return false
		}
		p.completeSourceReset(msg)
		return true
	}

	// Parent to the receive span while keeping the worker's context for
	// cancellation, so index operations below land in the message's trace.
	// The queue handoff stays inside one process, so the parent is local: a
	// remote parent selects a different ParentBased sampler branch.
	if msg.SpanContext != nil {
		ctx = trace.ContextWithSpanContext(ctx, *msg.SpanContext)
	}

	ctx, span := p.startSpan(ctx, "events_process", consumerSpanOptions)
	defer span.End()
	// Guards every attribute block in this package: a span the sampler dropped
	// reports IsRecording false, so those messages skip attribute construction
	// too, not just the ones with tracing off.
	tracingActive := span.IsRecording()
	if tracingActive {
		span.SetAttributes(
			semconv.LLMDKVCacheEventsTopic(msg.Topic),
			semconv.LLMDKVCacheEventsPayloadSizeBytes(len(msg.Payload)),
		)
	}

	podID, modelName, batch, err := p.decode(ctx, msg)
	if err != nil {
		if tracingActive {
			span.SetStatus(codes.Error, err.Error())
		}
		logger.Error(err, "Failed to parse message")
		podID = msg.SourceEndpoint
		if podID == "" {
			podID = p.adapter.ShardingKey(msg)
		}
		active, clearErr := p.recoverFailedMessage(ctx, msg, podID, msg.SourceDataParallelRank)
		if !active {
			return true
		}
		if clearErr != nil {
			logger.Error(clearErr, "Failed to clear state after a KV-event decode error",
				"podIdentifier", podID)
		}
		return false
	}

	if msg.SourceEndpoint != "" {
		podID = msg.SourceEndpoint
		batch.DataParallelRank = msg.SourceDataParallelRank
	}
	if tracingActive {
		span.SetAttributes(
			semconv.LLMDKVCacheEventsPodID(podID),
			semconv.LLMDKVCacheEventsEventCount(len(batch.Events)),
		)
	}

	if err := p.processEventBatch(ctx, &batch, podID, modelName); err != nil {
		logger.Error(err, "KV-event batch did not apply completely", "podIdentifier", podID)
		active, clearErr := p.recoverFailedMessage(ctx, msg, podID, batch.DataParallelRank)
		if !active {
			return true
		}
		if clearErr != nil {
			logger.Error(clearErr, "Failed to clear state after a KV-event mutation error",
				"podIdentifier", podID)
		}
		return false
	}
	p.recordAppliedSource(msg)
	return true
}

func (p *Pool) recordAppliedSource(msg *RawMessage) {
	if msg.EventSourceID == "" {
		return
	}
	source := checkpointSource{
		EventEndpoint: msg.EventEndpoint, ServingEndpoint: msg.SourceEndpoint,
		DataParallelRank:    cloneOptionalInt(msg.SourceDataParallelRank),
		LastAppliedSequence: msg.Sequence, EventDigest: checkpointEventDigest(msg.Topic, msg.Payload),
	}
	p.appliedSourcesMu.Lock()
	if generation, found := p.sourceGenerations[msg.EventSourceID]; found &&
		msg.sourceGeneration != generation {
		p.appliedSourcesMu.Unlock()
		return
	}
	if _, retired := p.retiredGenerations[msg.EventSourceID][msg.sourceGeneration]; retired {
		p.appliedSourcesMu.Unlock()
		return
	}
	source.Generation = msg.sourceGeneration
	p.appliedSources[msg.EventSourceID] = source
	p.appliedSourcesMu.Unlock()
}

func (p *Pool) registerSource(eventSourceID string) uint64 {
	if eventSourceID == "" {
		return 0
	}
	generation := p.nextGeneration.Add(1)
	p.appliedSourcesMu.Lock()
	p.sourceGenerations[eventSourceID] = generation
	p.appliedSourcesMu.Unlock()
	return generation
}

func (p *Pool) retireSourceGeneration(eventSourceID string, generation uint64) {
	if eventSourceID == "" || generation == 0 {
		return
	}
	p.appliedSourcesMu.Lock()
	if p.retiredGenerations[eventSourceID] == nil {
		p.retiredGenerations[eventSourceID] = make(map[uint64]struct{})
	}
	p.retiredGenerations[eventSourceID][generation] = struct{}{}
	p.appliedSourcesMu.Unlock()
}

func (p *Pool) sourceGenerationIsRetired(eventSourceID string, generation uint64) bool {
	if eventSourceID == "" || generation == 0 {
		return false
	}
	p.appliedSourcesMu.RLock()
	_, retired := p.retiredGenerations[eventSourceID][generation]
	p.appliedSourcesMu.RUnlock()
	return retired
}

func (p *Pool) forgetSourceState(eventSourceID string) {
	p.checkpointMu.Lock()
	p.appliedSourcesMu.Lock()
	delete(p.appliedSources, eventSourceID)
	delete(p.invalidSources, eventSourceID)
	p.appliedSourcesMu.Unlock()
	p.checkpointMu.Unlock()
}

func (p *Pool) markSourceInvalid(eventSourceID string) uint64 {
	if eventSourceID == "" {
		return 0
	}
	version := p.nextInvalidation.Add(1)
	p.appliedSourcesMu.Lock()
	p.addSourceInvalidationVersionLocked(eventSourceID, version)
	p.appliedSourcesMu.Unlock()
	return version
}

func (p *Pool) addSourceInvalidationLocked(eventSourceID string) uint64 {
	version := p.nextInvalidation.Add(1)
	p.addSourceInvalidationVersionLocked(eventSourceID, version)
	return version
}

func (p *Pool) addSourceInvalidationVersionLocked(eventSourceID string, version uint64) {
	if p.invalidSources[eventSourceID] == nil {
		p.invalidSources[eventSourceID] = make(map[uint64]struct{})
	}
	p.invalidSources[eventSourceID][version] = struct{}{}
}

func (p *Pool) beginSourceReset(eventSourceID string) uint64 {
	return p.markSourceInvalid(eventSourceID)
}

func (p *Pool) completeSourceReset(msg *RawMessage) {
	if msg.EventSourceID == "" {
		return
	}
	p.appliedSourcesMu.Lock()
	if source, found := p.appliedSources[msg.EventSourceID]; found &&
		source.matches(msg.EventEndpoint, msg.SourceEndpoint, msg.ResetDataParallelRank) {
		delete(p.appliedSources, msg.EventSourceID)
	}
	for _, version := range msg.resetVersions {
		delete(p.invalidSources[msg.EventSourceID], version)
	}
	if len(p.invalidSources[msg.EventSourceID]) == 0 {
		delete(p.invalidSources, msg.EventSourceID)
	}
	p.appliedSourcesMu.Unlock()
}

func (p *Pool) invalidateFailedSource(
	ctx context.Context,
	msg *RawMessage,
	podID string,
	dataParallelRank *int,
) (uint64, bool, error) {
	p.appliedSourcesMu.Lock()
	defer p.appliedSourcesMu.Unlock()
	if msg.EventSourceID != "" && msg.sourceGeneration != 0 {
		if _, retired := p.retiredGenerations[msg.EventSourceID][msg.sourceGeneration]; retired {
			return 0, false, nil
		}
	}
	var err error
	if podID != "" {
		err = p.clearSource(ctx, podID, dataParallelRank)
	}
	if msg.EventSourceID == "" {
		return 0, true, err
	}
	return p.addSourceInvalidationLocked(msg.EventSourceID), true, err
}

func (p *Pool) recoverFailedMessage(
	ctx context.Context,
	msg *RawMessage,
	podID string,
	dataParallelRank *int,
) (bool, error) {
	if msg.onFailure != nil {
		return msg.onFailure(ctx, podID, dataParallelRank)
	}
	_, active, err := p.invalidateFailedSource(ctx, msg, podID, dataParallelRank)
	return active, err
}

func cloneOptionalInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// decode spans the adapter's payload decode. It wraps the call rather than the
// EngineAdapter itself so out-of-tree adapters need no signature change.
//
//nolint:gocritic // unnamedResult: named returns conflict with nonamedreturns linter
func (p *Pool) decode(ctx context.Context, msg *RawMessage) (string, string, EventBatch, error) {
	_, span := p.startSpan(ctx, "events_decode", internalSpanOptions)
	defer span.End()

	podID, modelName, batch, err := p.adapter.ParseMessage(msg)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return podID, modelName, batch, err
	}

	// pod_id and event_count live on events_process, which holds the effective
	// pod after the SourceEndpoint override. Repeating the pre-override pod
	// under the same key would give one attribute two meanings in one trace.
	if span.IsRecording() {
		span.SetAttributes(semconv.GenAIRequestModel(modelName))
	}

	return podID, modelName, batch, nil
}

func (p *Pool) clearPod(ctx context.Context, podIdentifier string) error {
	debugLogger := log.FromContext(ctx).V(logging.DEBUG)
	if err := p.index.Clear(ctx, podIdentifier); err != nil {
		debugLogger.Error(err, "Failed to clear pod from index",
			"podIdentifier", podIdentifier)
		return err
	}
	p.dedup.clear(podIdentifier)
	p.groupCatalog.Clear(podIdentifier)
	return nil
}

func (p *Pool) clearSource(ctx context.Context, podIdentifier string, dataParallelRank *int) error {
	if dataParallelRank == nil {
		return p.clearPod(ctx, podIdentifier)
	}
	return p.clearRank(ctx, podIdentifier, *dataParallelRank)
}

func (p *Pool) clearRank(ctx context.Context, podIdentifier string, dataParallelRank int) error {
	debugLogger := log.FromContext(ctx).V(logging.DEBUG)
	if err := kvblock.ClearDataParallelRank(ctx, p.index, podIdentifier, dataParallelRank); err != nil {
		debugLogger.Error(err, "Failed to clear data-parallel rank from index",
			"podIdentifier", podIdentifier, "dataParallelRank", dataParallelRank)
		return err
	}
	p.dedup.clearRank(podIdentifier, dataParallelRank)
	return nil
}

// realignExtraFeatures converts per-engine-block extra features to per-canonical-block
// granularity so that len(result) matches the canonical chunk count expected by
// TokensToKVBlockKeys.
//
// For 1:many (engine BS > canonical BS): each engine block's features are replicated
// to all its constituent canonical sub-blocks.
// For many:1 (engine BS < canonical BS): features from multiple engine blocks are
// merged (union of MMHashes) into each canonical block.
//
// When all entries are nil (text-only prompts), this simply produces a nil-filled
// slice of the correct length.
func realignExtraFeatures(engineFeatures []*kvblock.BlockExtraFeatures, canonicalBlockCount int) []*kvblock.BlockExtraFeatures {
	engineBlockCount := len(engineFeatures)
	if canonicalBlockCount == 0 {
		return nil
	}
	if engineBlockCount == 0 || engineBlockCount == canonicalBlockCount {
		return engineFeatures
	}

	canonical := make([]*kvblock.BlockExtraFeatures, canonicalBlockCount)

	if engineBlockCount < canonicalBlockCount {
		// 1:many -> replicate each engine feature to its canonical sub-blocks
		for i := range canonicalBlockCount {
			engineIdx := i * engineBlockCount / canonicalBlockCount
			canonical[i] = engineFeatures[engineIdx]
		}
	} else {
		// many:1 -> merge constituent engine features into each canonical block
		for i, ef := range engineFeatures {
			canonicalIdx := i * canonicalBlockCount / engineBlockCount
			if ef == nil {
				continue
			}
			if canonical[canonicalIdx] == nil {
				canonical[canonicalIdx] = &kvblock.BlockExtraFeatures{}
			}
			canonical[canonicalIdx].MMHashes = append(
				canonical[canonicalIdx].MMHashes, ef.MMHashes...)
		}
	}

	return canonical
}

// handleDeviceTierUpdate handles offloading/location-only events (e.g., DeviceTier=CPU
// with no tokens). It resolves existing request keys from the engine→request mapping and
// adds the new PodEntry so the EPP tracks which device tiers hold each block.
//
// It returns true only when at least one engine key resolved and the resulting
// PodEntry was added to the index, so the caller knows the store took effect
// and can reference-count it.
func (p *Pool) handleDeviceTierUpdate(
	ctx context.Context, tokens []uint32, engineKeys []kvblock.BlockHash,
	podEntries []kvblock.PodEntry, podIdentifier, deviceTier string,
) (bool, error) {
	debugLogger := log.FromContext(ctx).V(logging.DEBUG)

	// Only attempt resolution when tokens are truly absent; partial-block
	// events (tokens < blockSize) should just be skipped.
	if len(tokens) != 0 || len(engineKeys) == 0 {
		return false, nil
	}

	seen := make(map[kvblock.BlockHash]struct{})
	var resolvedKeys []kvblock.BlockHash
	for _, ek := range engineKeys {
		rk, err := p.index.GetRequestKey(ctx, ek)
		if err != nil {
			continue
		}
		if _, ok := seen[rk]; !ok {
			seen[rk] = struct{}{}
			resolvedKeys = append(resolvedKeys, rk)
		}
	}

	if len(resolvedKeys) == 0 {
		debugLogger.Info("no indexed engine keys found for device-tier update, skipping",
			"podIdentifier", podIdentifier, "engineKeyCount", len(engineKeys))
		return false, nil
	}

	if err := p.index.Add(ctx, nil, resolvedKeys, podEntries); err != nil {
		debugLogger.Error(err, "Failed to add device-tier update to index",
			"podIdentifier", podIdentifier, "deviceTier", deviceTier)
		return false, fmt.Errorf("add device-tier update to index: %w", err)
	}
	return true, nil
}

// processEventBatch processes a batch of events using type switches.
func (p *Pool) processEventBatch(ctx context.Context, batch *EventBatch, podIdentifier, modelName string) error {
	debugLogger := log.FromContext(ctx).V(logging.DEBUG)
	debugLogger.V(logging.TRACE).Info("Processing event batch",
		"podID", podIdentifier,
		"modelName", modelName,
		"eventCount", len(batch.Events))

	unknownCount := 0
	for _, event := range batch.Events {
		if _, ok := event.(*UnknownEvent); ok {
			unknownCount++
		}
	}
	if unknownCount > 0 {
		metrics.UnknownEvents.Add(float64(unknownCount))
		debugLogger.Info("Unknown events invalidate the source cache state",
			"podIdentifier", podIdentifier,
			"dataParallelRank", batch.DataParallelRank,
			"unknownEventCount", unknownCount)
		if batch.DataParallelRank == nil {
			return p.clearPod(ctx, podIdentifier)
		} else {
			return p.clearRank(ctx, podIdentifier, *batch.DataParallelRank)
		}
	}

	var batchErrs []error
	// Process each event in the batch
	for _, genericEvent := range batch.Events {
		switch ev := genericEvent.(type) {
		case *BlockStoredEvent:
			deviceTier := normalizeDeviceTier(ev.DeviceTier)

			// Scope for reference-counting this store against duplicate removes.
			// Mirrors the index eviction identity.
			storeScope := blockScope{
				podIdentifier:    podIdentifier,
				deviceTier:       deviceTier,
				groupIdx:         groupIdxOrNoGroup(ev.GroupIdx),
				dataParallelRank: dataParallelRankOrNone(batch.DataParallelRank),
			}

			// Use LoRA name as model identifier if available, otherwise fall back to base model name.
			effectiveModelName := modelName
			if ev.LoraName != nil && *ev.LoraName != "" {
				effectiveModelName = *ev.LoraName
			}

			// Create PodEntry for this specific event's device tier.
			podEntries := []kvblock.PodEntry{{
				PodIdentifier:    podIdentifier,
				DeviceTier:       deviceTier,
				DataParallelRank: batch.DataParallelRank,
			}}
			if ev.GroupIdx != nil {
				g := kvblock.GroupID(*ev.GroupIdx)
				if ev.KVCacheSpecKind == "" {
					if meta, found := p.groupCatalog.Get(podIdentifier, g); found {
						ev.KVCacheSpecKind = KVCacheSpecKind(meta.Kind)
					}
				}
				podEntries[0].HasGroup = true
				podEntries[0].GroupIdx = g
			}

			if metadata, valid := blockStoredEventGroupMetadata(ev); valid {
				p.groupCatalog.Learn(podIdentifier, kvblock.GroupID(*ev.GroupIdx), metadata)
			}

			if digestible, reason := blockStoredEventDigestible(ev); !digestible {
				metrics.KVEventStoresSkipped.WithLabelValues(cacheKindLabel(ev.KVCacheSpecKind), reason).Inc()
				log.FromContext(ctx).V(logging.TRACE).Info("Skipping KV cache store event",
					"podIdentifier", podIdentifier,
					"groupIdx", ev.GroupIdx,
					"cacheKind", ev.KVCacheSpecKind,
					"reason", reason,
					"numTokens", len(ev.Tokens),
					"numBlockHashes", len(ev.BlockHashes),
					"blockSize", ev.BlockSize)
				continue
			}
			engineKeys := make([]kvblock.BlockHash, len(ev.BlockHashes))
			for i, hash := range ev.BlockHashes {
				engineKeys[i] = kvblock.BlockHash(hash)
			}

			parentRequestKey := kvblock.EmptyBlockHash
			if ev.ParentHash != 0 {
				parentEngineKey := kvblock.BlockHash(ev.ParentHash)
				key, err := p.index.GetRequestKey(ctx, parentEngineKey)
				if err != nil {
					debugLogger.Error(err, "Failed to get request key for parent block",
						"parentEngineKey", parentEngineKey,
						"effectiveModelName", effectiveModelName,
						"groupIdx", ev.GroupIdx,
						"cacheKind", ev.KVCacheSpecKind,
						"numTokens", len(ev.Tokens),
						"numBlockHashes", len(ev.BlockHashes),
						"blockSize", ev.BlockSize)
					batchErrs = append(batchErrs, fmt.Errorf("get parent request key: %w", err))
					continue
				}
				parentRequestKey = key
			}

			var extraFeatures []*kvblock.BlockExtraFeatures
			if ev.ExtraKeys != nil {
				var err error
				extraFeatures, err = kvblock.ParseRawExtraKeys(ev.ExtraKeys)
				if err != nil {
					debugLogger.Error(err, "Failed to parse extra keys",
						"podIdentifier", podIdentifier)
					batchErrs = append(batchErrs, fmt.Errorf("parse extra keys: %w", err))
					continue
				}
			}

			// Realign extraFeatures from engine-block granularity to canonical-block
			// granularity. ParseRawExtraKeys returns one entry per engine block, but
			// TokensToKVBlockKeys expects one entry per canonical block.
			if extraFeatures != nil {
				canonicalBlockCount := len(ev.Tokens) / p.tokenProcessor.BlockSize()
				if canonicalBlockCount == 0 {
					// Tokens don't fill a complete canonical block; no realignment needed
					// since TokensToKVBlockKeys will produce zero keys anyway.
					extraFeatures = nil
				} else if len(extraFeatures) != canonicalBlockCount {
					extraFeatures = realignExtraFeatures(extraFeatures, canonicalBlockCount)
				}
			}

			traceLogger := log.FromContext(ctx).V(logging.TRACE)
			if traceLogger.Enabled() {
				nonNil := 0
				for _, ef := range extraFeatures {
					if ef != nil {
						nonNil++
					}
				}
				traceLogger.Info("BlockStored extra_features",
					"podIdentifier", podIdentifier,
					"hasExtraKeys", ev.ExtraKeys != nil,
					"parsedBlockCount", len(extraFeatures),
					"nonNilBlocks", nonNil,
					"numTokens", len(ev.Tokens),
					"numEngineKeys", len(ev.BlockHashes))
				for bIdx, ef := range extraFeatures {
					if ef != nil {
						traceLogger.Info("BlockStored block extra",
							"podIdentifier", podIdentifier,
							"blockIdx", bIdx,
							"mmHashes", fmt.Sprintf("%+v", ef.MMHashes))
					}
				}
			}

			// Compute request keys at canonical block size (= BlockSize)
			requestKeys, err := p.tokenProcessor.TokensToKVBlockKeys(
				parentRequestKey, ev.Tokens, effectiveModelName, extraFeatures)
			if err != nil {
				debugLogger.Error(err, "Failed to generate request keys",
					"podIdentifier", podIdentifier, "effectiveModelName", effectiveModelName)
				batchErrs = append(batchErrs, fmt.Errorf("generate request keys: %w", err))
				continue
			}

			if len(requestKeys) == 0 {
				applied, err := p.handleDeviceTierUpdate(
					ctx, ev.Tokens, engineKeys, podEntries, podIdentifier, deviceTier,
				)
				if err != nil {
					batchErrs = append(batchErrs, err)
				} else if applied {
					p.dedup.trackStore(storeScope, ev.BlockHashes)
				}
				continue
			}

			// Index.Add infers the engine->request mapping from the ratio of
			// len(engineKeys) to len(requestKeys) (1:1, many:1, or 1:many).
			if err := p.index.Add(ctx, engineKeys, requestKeys, podEntries); err != nil {
				debugLogger.Error(err, "Failed to add event to index",
					"podIdentifier", podIdentifier, "event", ev)
				batchErrs = append(batchErrs, fmt.Errorf("add event to index: %w", err))
				continue
			}
			p.dedup.trackStore(storeScope, ev.BlockHashes)

		case *BlockRemovedEvent:
			deviceTier := normalizeDeviceTier(ev.DeviceTier)
			if ev.GroupIdx != nil {
				groupIdx := kvblock.GroupID(*ev.GroupIdx)
				meta, found := p.groupCatalog.Get(podIdentifier, groupIdx)
				if !found || !isPrefixIndexableSpecKind(KVCacheSpecKind(meta.Kind)) {
					reason := "unsupported_cache_kind"
					if !found {
						reason = "unknown_group"
					}
					metrics.KVEventRemovalsSkipped.WithLabelValues(
						cacheKindLabel(KVCacheSpecKind(meta.Kind)), reason).Inc()
					log.FromContext(ctx).V(logging.TRACE).Info("Skipping KV cache remove event",
						"podIdentifier", podIdentifier,
						"groupIdx", groupIdx,
						"cacheKind", meta.Kind,
						"groupKnown", found)
					continue
				}
			}

			// Create PodEntry for this specific event's device tier.
			podEntries := []kvblock.PodEntry{{
				PodIdentifier:    podIdentifier,
				DeviceTier:       deviceTier,
				DataParallelRank: batch.DataParallelRank,
			}}
			if ev.GroupIdx != nil {
				podEntries[0].HasGroup = true
				podEntries[0].GroupIdx = kvblock.GroupID(*ev.GroupIdx)
			}

			// Reference-count duplicate removes: vLLM chunk-mode offloading can
			// re-announce a shared constituent hash across overlapping chunks, so
			// only forward a hash to the index once no outstanding store still
			// references it. Unknown hashes pass through (Evict is a no-op).
			removeScope := blockScope{
				podIdentifier:    podIdentifier,
				deviceTier:       deviceTier,
				groupIdx:         groupIdxOrNoGroup(ev.GroupIdx),
				dataParallelRank: dataParallelRankOrNone(batch.DataParallelRank),
			}
			hashesToEvict := p.dedup.filterRemove(removeScope, ev.BlockHashes)

			// Observe how many constituent block hashes were forwarded vs.
			// suppressed (these count block hashes, not BlockRemoved events).
			if forwarded := len(hashesToEvict); forwarded > 0 {
				metrics.DedupRemovedHashesForwarded.Add(float64(forwarded))
			}
			if suppressed := len(ev.BlockHashes) - len(hashesToEvict); suppressed > 0 {
				metrics.DedupRemovedHashesSuppressed.Add(float64(suppressed))
				log.FromContext(ctx).V(logging.TRACE).Info("Suppressed duplicate block removals",
					"podIdentifier", podIdentifier, "deviceTier", deviceTier,
					"received", len(ev.BlockHashes), "forwarded", len(hashesToEvict), "suppressed", suppressed)
			}

			// Iterate over the surviving hashes and evict each key.
			// The Index handles engine->request key resolution internally for both
			// 1:1 (legacy) and 1:many (canonical) mappings.
			for _, hash := range hashesToEvict {
				engineKey := kvblock.BlockHash(hash)
				if err := p.index.Evict(ctx, engineKey, kvblock.EngineKey, podEntries); err != nil {
					debugLogger.Error(err, "Failed to evict engine key from index",
						"podIdentifier", podIdentifier, "engineKey", engineKey)
					batchErrs = append(batchErrs, fmt.Errorf("evict engine key %s: %w", engineKey.String(), err))
					continue
				}
			}

		case *AllBlocksClearedEvent:
			debugLogger.Info("All blocks cleared event received",
				"podIdentifier", podIdentifier,
				"deviceTier", ev.DeviceTier,
				"modelName", modelName)

			// AllBlocksCleared resets the emitting engine's entire prefix cache. A
			// data-parallel rank must not clear sibling ranks sharing the pod.
			// Index.Clear cannot scope by tier, so if an engine ever starts setting
			// DeviceTier (a tier-scoped reset), this would over-wipe the other tiers.
			// Surface that here so the regression does not pass silently.
			if ev.DeviceTier != "" {
				debugLogger.Info("AllBlocksCleared carried a device tier; clearing all tiers "+
					"anyway (tier-scoped clear is not supported)",
					"podIdentifier", podIdentifier, "deviceTier", ev.DeviceTier)
			}
			if batch.DataParallelRank == nil {
				if err := p.clearPod(ctx, podIdentifier); err != nil {
					batchErrs = append(batchErrs, err)
				}
			} else {
				if err := p.clearRank(ctx, podIdentifier, *batch.DataParallelRank); err != nil {
					batchErrs = append(batchErrs, err)
				}
			}

		}
	}
	return errors.Join(batchErrs...)
}
