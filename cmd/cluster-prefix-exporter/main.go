// Copyright 2026 The llm-d Authors.
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

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvevents"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("cluster-prefix-exporter", flag.ContinueOnError)
	checkpoint := flags.String("checkpoint", "", "input KV index checkpoint")
	checkpointDirectory := flags.String("checkpoint-directory", "", "directory of per-writer checkpoints")
	output := flags.String("output", "", "output cluster prefix snapshot")
	fingerprint := flags.String("fingerprint", "", "checkpoint configuration fingerprint")
	cluster := flags.String("cluster", "", "cluster name")
	model := flags.String("model", "", "model name")
	blockSize := flags.Int("block-size", 0, "KV block size in tokens")
	hashSeed := flags.String("hash-seed", "", "block hash seed")
	hashAlgorithm := flags.String("hash-algorithm", kvblock.HashAlgorithmCBORFNV, "block hash algorithm")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || (*checkpoint == "") == (*checkpointDirectory == "") ||
		*output == "" || *fingerprint == "" ||
		*cluster == "" || *model == "" || *blockSize <= 0 {
		return errors.New("one checkpoint source, output, fingerprint, cluster, model, and positive block-size are required")
	}
	meta := kvblock.ClusterPrefixSnapshotMetadata{
		Cluster: *cluster, Model: *model, BlockSizeTokens: *blockSize,
		HashSeed: *hashSeed, HashAlgorithm: *hashAlgorithm,
	}
	var result kvblock.ClusterPrefixSnapshotResult
	var err error
	selected := *checkpoint
	if *checkpointDirectory != "" {
		selected, result, err = kvevents.ExportLatestClusterPrefixSnapshot(
			*checkpointDirectory, *output, *fingerprint, meta)
	} else {
		result, err = kvevents.ExportClusterPrefixSnapshot(*checkpoint, *output, *fingerprint, meta)
	}
	if err != nil {
		return err
	}
	fmt.Printf("exported %d backends and %d keys from %s to %s\n",
		result.Backends, result.Keys, selected, *output)
	return nil
}
