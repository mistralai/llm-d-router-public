# Precise Prefix Cache Producer

**Type:** `precise-prefix-cache-producer`

DataProducer that owns the precise KV-block index and publishes
per-endpoint `PrefixCacheMatchInfo`. Pairs with the generic
[`prefix-cache-scorer`](../../../scheduling/scorer/prefix/); the scorer
must reference this producer by name:

```yaml
- type: prefix-cache-scorer
  parameters:
    prefixMatchInfoProducerName: precise-prefix-cache-producer
```

Without the `prefixMatchInfoProducerName` field, the scorer falls back
to the auto-spawned approx producer.

Pipeline per request:
- Consume `TokenizedPrompt` from `token-producer`.
- Hash tokens → KV-block keys → `kvblock.Index.Lookup`.
- Write `PrefixCacheMatchInfo(matchBlocks, totalBlocks, blockSizeTokens)` per endpoint, including the unweighted cached-block count and its per-device-tier breakdown.
- (`PreRequest`) Speculative-index the selected endpoint(s) with TTL eviction.
- (`EndpointExtractor`) Per-pod, per-rank ZMQ subscriber lifecycle on add/delete.

Requires `TokenizedPrompt` on the request — set by a `token-producer`
upstream. No-op otherwise.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tokenProcessorConfig` | object | `kvblock.DefaultTokenProcessorConfig()` | KV-block hashing for the EPP-recomputed keys (block size, hash seed). |
| `indexerConfig` | object | `kvcache.NewDefaultConfig()` | `kvcache.Indexer` config. |
| `kvEventsConfig` | object | `kvevents.DefaultConfig()` | KV-events pool config. |
| `speculativeIndexing` | bool | `false` | Seed predicted entries on routing decisions. |
| `speculativeTTL` | duration | `2s` | TTL for speculative entries. |

Set `kvEventsConfig.engineType` to `sglang` for SGLang KV-events. It defaults
to `vllm` when omitted.

For vLLM Internal or Hybrid LB, where all local DP ranks share one HTTP
endpoint, configure `dp-rank-header-handler`. EPP detects each pod's local rank
count from the `engine` labels on `vllm:num_requests_running` and exposes each
rank to the scheduler as a logical endpoint. Pods with different DP sizes can
share one InferencePool.

The chart value is an optional fallback when metrics do not expose the rank
count:

```yaml
# Helm values
router:
  modelServers:
    dataParallelSize: 8
```

The chart renders `--endpoint-data-parallel-size=8`. When running EPP without
the chart, pass that flag directly. Automatic discovery and the fallback apply
only to a shared-port deployment with exactly one target port. Enable pod
discovery for KV events and set the base publisher port:

```yaml
kvEventsConfig:
  discoverPods: true
  podDiscoveryConfig:
    socketPort: 5557
```

Endpoint discovery creates one schedulable endpoint per `(pod, rank)`, all
sharing the pod's serving address. The producer creates one subscriber per
logical endpoint on `socketPort + rank` and keeps precise-cache scores separate
by rank. Configure `dp-rank-header-handler` so the rank of the selected logical
endpoint is sent to vLLM through `x-data-parallel-rank`.

The legacy one-endpoint-per-pod configuration remains supported when
`dp-rank-header-handler` is absent. Set
`kvEventsConfig.podDiscoveryConfig.dataParallelSize` greater than `1` to make
the producer create all rank subscribers from one endpoint per pod. It
collapses rank scores to that endpoint and exposes the cache-winning rank to
the header handler. Requests without a precise-cache winner fall back to
vLLM's internal balancing.

For External LB, leave `--endpoint-data-parallel-size` and the producer's
`dataParallelSize` at their defaults of `1`, and do not configure
`dp-rank-header-handler`. Every rank is already exposed as a separate serving
endpoint, so endpoint selection addresses the rank directly.

The KV-event `data_parallel_rank` must be the rank accepted by
`x-data-parallel-rank` on that serving endpoint. A wide-EP deployment where
events carry global ranks but the shared frontend accepts pod-local ranks needs
an explicit global-to-local translation and is not supported by this option.

See [llm-d-kv-cache/docs/configuration.md](https://github.com/llm-d/llm-d-kv-cache/blob/main/docs/configuration.md)
for nested parameter details.

## Engine compatibility

Block keys are recomputed by the EPP from `TokenizedPrompt` (tokens, model,
multimodal features, cache salt) on both the lookup path and the KV-event
ingestion path, using this plugin's `tokenProcessorConfig`. The engine's own
block hashes serve only as opaque keys for the engine-to-request mapping, so
`blockSize`/`hashSeed` need not match the engine.

The cross-engine requirement is that the engine emits, in its KV-events, the
hash-affecting inputs the EPP hashes: `token_ids`, and `extra_keys` carrying
multimodal identifiers and `cache_salt`. An input the engine omits from
`extra_keys` is absent on the event side, so requests carrying it do not
correlate.

| Engine | `extra_keys` in KV-events | `cache_salt` |
|--------|---------------------------|--------------|
| vLLM | emitted | in block-0 `extra_keys`; salted prefixes isolated and precise-routed |
| SGLang | not emitted | baked into engine block hashes but not surfaced; salted requests are precise-cache misses until SGLang emits `extra_keys` |

Salt isolation is enforced by the engine regardless; the above affects only
routing accuracy for salted requests.
