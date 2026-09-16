# Data Parallel Handlers

## Data Parallel Profile Handler

**Type:** `data-parallel-profile-handler`

> **Deprecated:** Use `single-profile-handler` with Istio >= 1.28.1 instead. See [single/](../single/).

Provides a profile handler for data-parallel inference routing, where a request is scheduled to one pod among multiple replicas serving the same model. Injects the `X-Data-Parallel-Endpoint` header pointing to the selected pod and rewrites the target port to `primaryPort`.

**Constraints:**
- Requires exactly one scheduling profile in the config.

**Parameters:**
- `primaryPort` (int, optional, default: `8000`): Primary service port (1–65535).

**Configuration Example:**
```yaml
plugins:
  - type: data-parallel-profile-handler
    name: dp-handler
    parameters:
      primaryPort: 8000
```

**Migration:** Replace with `single-profile-handler` (requires Istio >= 1.28.1):

**Before:**
```yaml
plugins:
  - type: data-parallel-profile-handler
    parameters:
      primaryPort: 8000
```

**After:**
```yaml
plugins:
  - type: single-profile-handler
```

## DP Rank Header Handler

**Type:** `dp-rank-header-handler`

Pins a request to the selected logical endpoint by setting
`x-data-parallel-rank` after scheduling. Use it with vLLM Internal or Hybrid
load balancing, where multiple local data-parallel ranks share one serving
port.

The handler enables per-pod rank-count discovery from the `engine` labels on
`vllm:cache_config_info`. The `--endpoint-data-parallel-size` flag supplies a
fallback rank count while metrics are unavailable. Shared-port data parallelism
requires exactly one target port.

The plugin is Alpha and requires `--allow-experimental-plugins`.

```yaml
plugins:
  - type: dp-rank-header-handler
```

The Helm chart configures the handler and the experimental-plugin flag when
`router.modelServers.dataParallelSize` is greater than `1` and the generated
`default-plugins.yaml` is used. A custom plugin configuration must include the
handler. To use automatic discovery with the default fallback of `1`, also set
`router.epp.flags.allow-experimental-plugins: true`.

Do not use this handler with vLLM External load balancing. Each rank already
has its own network endpoint in that mode.

---

## Related Documentation
- [SingleProfileHandler](../single/)
- [Disagg Profile Handler](../disagg/)
