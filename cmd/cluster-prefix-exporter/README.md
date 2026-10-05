# Cluster Prefix Exporter

This command writes a cluster prefix snapshot from one KV index checkpoint.
The snapshot records confirmed request keys by pod and data-parallel rank. The
cluster prefix producer reads the output to score peer clusters.

```sh
go run ./cmd/cluster-prefix-exporter \
  --checkpoint /checkpoints/checkpoint.bin \
  --output /snapshots/cluster-a-model-a.bin \
  --fingerprint <checkpoint-configuration-sha256> \
  --cluster cluster-a \
  --model model-a \
  --block-size 16 \
  --hash-seed <seed> \
  --hash-algorithm cbor-fnv
```

The input checkpoint must contain only the named model. Request keys do not
retain a model name, so the exporter cannot separate models in a mixed index.
The supplied block size, seed, and algorithm must match the source index. The
checkpoint fingerprint and checksum are checked before the output file is
replaced. The snapshot timestamp is the checkpoint creation time.

Run the command after each checkpoint write. It replaces the output through a
temporary file in the same directory. Publish that file to the federated EPP
through the snapshot path configured for its cluster prefix producer.
