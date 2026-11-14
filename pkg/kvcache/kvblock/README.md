# KV-Block Index

The block-level store that maps KV-block keys to the pods holding them, plus the
token-to-block-key processing that feeds it. This is the shared state the
[`kvcache.Indexer`](../README.md) reads and the [`kvevents`](../../kvevents/README.md)
subscriber writes.

## What It Does

- **Token processing.** `TokenProcessor` (backed by `ChunkedTokenDatabase`)
  turns a token sequence into KV-block keys: it chunks tokens into fixed-size
  blocks and hashes each block, chaining the previous block's hash so a key
  encodes its full prefix. `ComputeBlockExtraFeatures` folds multimodal
  metadata into per-block `BlockExtraFeatures` that taint the hash. The first
  block's features can also carry the request's cache salt.
- **Block index.** `Index` maps each block key to the set of pods (`PodEntry`,
  carrying a pod identifier and device tier) that currently hold it. Producers
  `Add` and `Evict` entries; the indexer's `Lookup` reads them.
- **Ordered walk.** `KeyWalker` is an optional capability for callers that
  fold entries in key order: it visits the requested keys in sequence,
  reporting misses, and lends each key's entries as `EntryRef` values that
  carry the index's pod and tier ordinals. The in-memory backends have it and
  the decorators keep it; the [`kvcache`](../README.md) prefix matcher walks
  when it can and falls back to `Lookup`.

## Backends

`NewIndex` selects the first configured backend in this order:

| Backend | Constructor | Use |
|---------|-------------|-----|
| Cost-aware memory | `NewCostAwareMemoryIndex` | Local index with cost-aware eviction. |
| Redis | `NewRedisIndex` | Shared index across EPP replicas (Redis-protocol server). |
| Otter | `NewOtterIndex` | Local index with concurrent location and translation caches. |
| In-memory | `NewInMemoryIndex` | Default local index bounded by key count. |

Two decorators wrap any backend: `NewTracedIndex` adds OpenTelemetry spans
(no-op when tracing is off) and `NewInstrumentedIndex` records the
[metrics](../metrics/README.md).

### Otter backend

Set `otterConfig` to select the Otter backend:

```yaml
otterConfig:
  size: 1000000
  podCacheSize: 128
```

`InMemoryIndex` uses LRU caches and remains the default. `OtterIndex` uses
concurrent caches for both request-key locations and engine-key translations,
with buffered access history and an approximate recency/frequency eviction
policy. Inserts and explicit removals are visible synchronously.
Capacity maintenance can temporarily exceed `size`, which limits
key count in each cache rather than bytes. Read-ahead lookups and eviction scans
do not record accesses; `Lookup` and `WalkKeys` record their visited prefixes.

`OtterIndex` owns its lookup, traversal, translation, and eviction methods.
It shares the pod-entry representation and ordinal interner with `InMemoryIndex`.
Both indexes retain per-key pod-entry locks and an Add/Evict ordering lock.
Parent translations must remain available for later KV events to extend a prefix,
so eviction policy affects event processing as well as routing cache hits.

## Grouped (HMA) Entries

`GroupCatalog` tracks group identity for heterogeneous-memory-aware (HMA)
models, where a block may be announced by a group of engines; eviction is
reference-counted at the group level so a block is removed only once no group
still references it.

## Key Types

| Symbol | Role |
|--------|------|
| `Index` | Block-key -> pods mapping (`Add` / `Lookup` / `Evict`). |
| `KeyWalker` / `EntryRef` | Optional ordered walk over requested keys; entries with pod and tier ordinals. |
| `TokenProcessor` | Tokens -> block keys; exposes `BlockSize`. |
| `BlockHash` | A single block key. |
| `PodEntry` | A pod holding a block, with its device tier. |
| `BlockExtraFeatures` / `ComputeBlockExtraFeatures` | Per-block multimodal metadata and cache salt folded into the hash. |
| `PlaceholderRange` | Placeholder-token range for a multimodal item. |

## Related Documentation

- [KV-Cache Indexer](../README.md) -- reads this index to score pods
- [KV-Events](../../kvevents/README.md) -- writes to this index from engine events
