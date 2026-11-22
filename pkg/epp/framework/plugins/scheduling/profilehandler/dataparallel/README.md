# DP Rank Header Handler

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
- [Single Profile Handler](../single/)
- [Disagg Profile Handler](../disagg/)
