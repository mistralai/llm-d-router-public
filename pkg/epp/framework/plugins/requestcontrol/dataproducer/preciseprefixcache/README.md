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
- (`EndpointExtractor`) Per-pod ZMQ subscriber lifecycle on add/delete.

Requires `TokenizedPrompt` on the request — set by a `token-producer`
upstream. No-op otherwise.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tokenProcessorConfig` | object | `kvblock.DefaultTokenProcessorConfig()` | KV-block hashing for the EPP-recomputed keys (block size, hash seed). |
| `indexerConfig` | object | `kvcache.NewDefaultConfig()` | `kvcache.Indexer` config. |
| `kvEventsConfig` | object | `kvevents.DefaultConfig()` | KV-events pool config. |
| `checkpointDirectory` | string | empty | Shared persistent directory for KV-event index checkpoints. |
| `checkpointWriterID` | string | pod hostname | Identity used for this EPP's checkpoint file. |
| `checkpointInterval` | duration | empty | Required interval between writes when `checkpointDirectory` is set. |
| `clusterSnapshotOutput` | string | empty | Output path for the binary cluster prefix snapshot. Requires checkpointing. |
| `clusterSnapshotCluster` | string | empty | Cluster name in the exported snapshot. |
| `clusterSnapshotModel` | string | empty | Model name in the exported snapshot. |
| `speculativeIndexing` | bool | `false` | Seed predicted entries on routing decisions. |
| `speculativeTTL` | duration | `2s` | TTL for speculative entries. |

Set `kvEventsConfig.engineType` to `sglang` for SGLang KV-events. It defaults
to `vllm` when omitted.

Checkpointing requires the in-memory index, per-pod discovery, and a positive
`replaySocketPort`. Each EPP atomically replaces its own file in the configured
directory. At startup, an EPP restores the newest valid file and tries older
files if that restore fails. The directory must be on persistent storage that
is shared by replacement EPP pods. A snapshot stores confirmed compact-index
entries, engine-key mappings, deduplication state, cache-group metadata, and the
last applied event sequence for each subscriber. Prefix matching filters
restored entries through the current request candidates, so entries for absent
endpoints cannot affect routing.

The checkpoint uses a versioned little-endian binary format with 64-bit section
offsets and a SHA-256 body checksum. A write streams to a temporary file, syncs
the file, and atomically replaces the prior checkpoint. Event application stops
at a message boundary during the write. Prefix lookups continue during the write.

Worker queues retain events received during a checkpoint and apply them in
source order after the write. `maxQueueDepth` limits the combined waiting
backlog to 65,536 messages by default. A full queue applies backpressure to the
subscribers. Monitor `llm_d_epp_kv_cache_events_pool_queue_depth`. Checkpoint
files are limited to 64 GiB.

Configure the cluster snapshot fields on one EPP replica per cluster and model.
After each successful checkpoint, that replica exports confirmed keys from the
checkpoint and atomically replaces the output file. The export uses the block
size, hash seed, and hash algorithm from `tokenProcessorConfig`. A failed export
leaves the last valid output file in place. The output directory must be
available to the EPP, and the snapshot file must be delivered to the
federated EPP.

Restore preserves cache membership but starts with new LRU recency. Replay
validates the saved topic and payload at each source boundary. The wire protocol
does not provide a publisher epoch, so a publisher restart is not detectable if
it reuses the same endpoint, sequence number, topic, and payload.

For vLLM Internal or Hybrid load balancing, configure
`dp-rank-header-handler`. Endpoint discovery creates one schedulable endpoint
per local rank while retaining the pod's shared serving address. The handler
sets `x-data-parallel-rank` to the selected endpoint's rank.

With pod KV-event discovery enabled, each logical endpoint subscribes to
`socketPort + rank` and `replaySocketPort + rank`. Cache entries, replay resets,
and endpoint deletion remain scoped to that rank.

When the serving endpoint belongs to a leader pod but each data-parallel rank's
KV-event socket belongs to a different pod, configure `rankPodMapping`. The
group label joins the serving endpoint to its worker pods. The rank label is the
worker index, and `ranksPerPod` maps each worker to a consecutive range of
global data-parallel ranks. Requests still target the leader endpoint and use
`x-data-parallel-rank`; only the KV-event transport uses the worker pod IP.
Each rank uses `socketPort + global rank` and
`replaySocketPort + global rank` on its worker pod.

```yaml
plugins:
  - type: precise-prefix-cache-producer
    parameters:
      kvEventsConfig:
        discoverPods: true
        podDiscoveryConfig:
          socketPort: 5557
          replaySocketPort: 5657
          podLabelSelector: app.kubernetes.io/instance=model-server
          podNamespace: default
          rankPodMapping:
            groupLabelKey: leaderworkerset.sigs.k8s.io/group-index
            rankLabelKey: leaderworkerset.sigs.k8s.io/worker-index
            ranksPerPod: 1
  - type: dp-rank-header-handler
```

The handler discovers rank counts from `vllm:cache_config_info` metrics.
`--endpoint-data-parallel-size` supplies a fallback count. The corresponding
Helm value is `router.modelServers.dataParallelSize`.

The handler is an Alpha plugin and requires
`--allow-experimental-plugins`. Do not configure it for vLLM External load
balancing, where each rank has a distinct network endpoint.

Set `kvEventsConfig.tracing` to `true` to emit OpenTelemetry spans for the
KV-event pipeline (`events_receive`, `events_process`, `events_decode`). It
defaults to `false`: KV events arrive at many times the inference request rate,
so with a shared head sampler always-on event spans crowd request traces out of
the exported volume. The EPP `--tracing` flag gates tracing as a whole, so this
field has no effect while that is off.

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
