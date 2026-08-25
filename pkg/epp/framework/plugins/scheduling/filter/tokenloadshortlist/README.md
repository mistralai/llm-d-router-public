# Token Load Shortlist Filter

**Type:** `token-load-shortlist-filter`

This filter restricts endpoint candidates using the in-flight token load tracked
by the EPP.

If one or more endpoints have exactly zero in-flight tokens, the filter keeps
all zero-token endpoints. Otherwise it keeps the `maxCandidates` endpoints with
the lowest token loads. All endpoints tied with the last retained endpoint are
kept, so the resulting candidate count can exceed `maxCandidates`.

Endpoints with missing, malformed, or negative load data are excluded when at
least one endpoint has valid load data. If no endpoint has valid load data, the
filter keeps all candidates.

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `maxCandidates` | Yes | Number of lowest-load endpoints to keep before cutoff ties are included. Must be greater than zero. |
| `inFlightLoadProducerName` | No | Name of the in-flight load producer to consume. |

```yaml
plugins:
- type: token-load-shortlist-filter
  name: token-load-shortlist
  parameters:
    maxCandidates: 4
- type: prefix-cache-scorer
  name: prefix-cache
- type: token-load-scorer
  name: token-load
- type: weighted-random-picker
  name: weighted-picker
schedulingProfiles:
- name: default
  plugins:
  - pluginRef: token-load-shortlist
  - pluginRef: prefix-cache
    weight: 10
  - pluginRef: token-load
    weight: 2
  - pluginRef: weighted-picker
```

The filter consumes `InFlightLoadDataKey`. The default `inflight-load-producer`
is injected automatically when no producer is configured.
