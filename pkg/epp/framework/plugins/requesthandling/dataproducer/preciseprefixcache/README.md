# Precise Prefix Cache Producer

**Type:** `precise-prefix-cache-producer`

## Native vLLM hashes

Set `hashEndpoint` to a running vLLM Python server's `/hash` endpoint to use
engine hashes for requests and KV events:

```yaml
- type: precise-prefix-cache-producer
  parameters:
    hashEndpoint: http://vllm:8000/hash
    hashTimeout: 5s
    speculativeIndexing: false
```

The vLLM server must enable prefix caching and `--enable-scale-out`. Its hash
settings, tokenizer, chat template, adapters and multimodal preprocessing must
match the pool. The KV-event topic's model name must equal the primary served
base-model name returned by `/hash`. Use grouped vLLM events. For pools with
different hash configurations, configure separate producers and hash services.

This mode has no token-producer dependency. It sends the forwarded OpenAI chat
or completion JSON unchanged, or sends parser-provided token IDs and multimodal
metadata for pretokenized inputs. It forwards the request's Authorization header.
Do not explicitly configure a token producer solely for this producer. Other
plugins that consume tokens can still request token production independently.

Each response supplies its effective hash and cache-group block sizes, including
DCP scaling. No canonical block size needs to match vLLM. Changed sizes take
effect on the next response without restarting the EPP. During a rollout with
incompatible hash settings, the selected hash service can produce conservative
misses for backends using a different configuration.

Stored and removed engine hashes address the index directly. Parent ancestry,
event token IDs, multimodal rehashing and `engineToRequestKeys` are unnecessary
on this path. Model and group namespaces remain separate. The router retains
its 64-bit hash representation, taking the low 64 bits of the full hexadecimal
hash, consistently with its KV-event decoder.

Scores report the minimum contiguous cached token coverage across full, MLA,
and sink-full attention groups, summed across prompts. Sparse state groups are
indexed but do not contribute to this score. Dense coverage on hybrid models
is a routing estimate; it does not establish that every recurrent or
sliding-window state needed for a scheduler cache hit is present. Partial tails
are retained in the index but full-group boundaries determine this score.
Residency is aggregated by pod address and port; the index does not distinguish
DP ranks sharing that address. Use separately addressable workers when routing
requires rank-specific cache predictions.

Speculative indexing is unsupported in native mode. Invalid or unavailable hash
responses omit the cache prediction, allowing other routing signals to operate.
Anthropic, Responses, prompt embeddings and legacy-translated tokens are
unsupported. Event loss still needs the existing resynchronization policy.
The mode adds a hash RPC; end-to-end throughput and latency must be measured
separately from event-ingestion performance.

## Token-based hashing

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
- (`EndpointExtractor`) Per-pod ZMQ subscriber lifecycle on add/delete.

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

SGLang EAGLE/MTP events describe cached positions as overlapping token pairs.
The index preserves their trailing lookahead and learns each pod's page size.
Request matching hashes complete bigram pages using the same format.
A page of `P` cached positions requires `P+1` matching request tokens.
Bigram endpoints report `PrefixCacheMatchInfo` in token-sized blocks.
This preserves cache-tier weights and exact cached-token counts across page sizes.
Plain-token workers use `tokenProcessorConfig.blockSizeTokens`.

Set `kvEventsConfig.tracing` to `true` to emit OpenTelemetry spans for the
KV-event pipeline (`events_receive`, `events_process`, `events_decode`). It
defaults to `false`: KV events arrive at many times the inference request rate,
so with a shared head sampler always-on event spans crowd request traces out of
the exported volume. The EPP `--tracing` flag gates tracing as a whole, so this
field has no effect while that is off.

Set `kvEventsConfig.podDiscoveryConfig.snapshotSocketPort` to the port where
vLLM serves KV-event snapshots (offset by data-parallel rank like `socketPort`)
to warm a pod's index from its current cache state. This is experimental: it
requires a vLLM build with
[vllm-project/vllm#57789](https://github.com/vllm-project/vllm/pull/57789),
whose wire format can still change.

A subscriber that joins mid-stream, or sees the engine restart, loads the
snapshot instead of replaying buffered events, then follows live events after
the snapshot's sequence number. A restart shows as a sequence that moves
backwards or as a new publisher identity, which vLLM sends with every batch
and snapshot. Without `replaySocketPort`, a gap in the event sequence reloads
the snapshot as well. While the snapshot fails, is unavailable or comes from
another publisher identity, the pod is indexed from live events alone, as it
is with neither port set, and the load is retried after the replay cooldown.
A snapshot that loads replaces that state.

See [llm-d-kv-cache/docs/configuration.md](https://github.com/llm-d/llm-d-kv-cache/blob/main/docs/configuration.md)
for nested parameter details.

## Engine compatibility

Block keys are recomputed by the EPP from `TokenizedPrompt` (tokens, model,
multimodal features, cache salt) on both the lookup path and the KV-event
ingestion path, using this plugin's `tokenProcessorConfig`. The engine's own
block hashes serve only as opaque keys for the engine-to-request mapping, so
`blockSizeTokens`/`hashSeed` need not match the engine.

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
