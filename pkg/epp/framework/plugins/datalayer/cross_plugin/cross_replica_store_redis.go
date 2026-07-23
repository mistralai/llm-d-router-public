/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datalayer

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
	ctrl "sigs.k8s.io/controller-runtime"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

func init() {
	gob.Register(&attrconcurrency.InFlightLoad{})
}

const RedisStateStoreType = "redis-state-store"

var _ fwkdl.CrossReplicaStore = (*RedisStateStore)(nil)

type redisConfig struct {
	Address  string `json:"address"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

// RedisStateStore is a CrossReplicaStore backed by Redis for cross-replica
// state sharing. Each replica writes under its own replicaID key prefix;
// Get aggregates values across all replicas using the caller-supplied
// aggregate function.
type RedisStateStore struct {
	typedName fwkplugin.TypedName
	replicaID string
	client    *redis.Client
}

func RedisStateStoreFactory(name string, params *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	var cfg redisConfig
	if params != nil {
		if err := params.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("redis-state-store: invalid parameters: %w", err)
		}
	}
	if cfg.Address == "" {
		cfg.Address = "localhost:6379"
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return &RedisStateStore{
		typedName: fwkplugin.TypedName{Type: RedisStateStoreType, Name: name},
		replicaID: hostname,
		client:    client,
	}, nil
}

func (s *RedisStateStore) TypedName() fwkplugin.TypedName {
	return s.typedName
}

func (s *RedisStateStore) replicaKey(key fwkdl.StateKey, endpointID string) string {
	return string(key) + ":" + endpointID + ":" + s.replicaID
}

func (s *RedisStateStore) scanPattern(key fwkdl.StateKey, endpointID string) string {
	return string(key) + ":" + endpointID + ":*"
}

type gobValue struct{ Value any }

func gobEncode(value any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(gobValue{Value: value}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(data []byte) (any, error) {
	var w gobValue
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return nil, err
	}
	return w.Value, nil
}

func (s *RedisStateStore) Set(ctx context.Context, key fwkdl.StateKey, endpointID string, value any) error {
	logger := ctrl.LoggerFrom(ctx)
	data, err := gobEncode(value)
	if err != nil {
		return fmt.Errorf("redis-state-store: encode: %w", err)
	}
	if err := s.client.Set(ctx, s.replicaKey(key, endpointID), data, 0).Err(); err != nil {
		return err
	}
	if v := logger.V(logutil.DEBUG); v.Enabled() {
		v.Info("redis-state-store: Set", "key", string(key), "endpoint", endpointID, "replica", s.replicaID, "value", fmt.Sprintf("%+v", value))
	}
	return nil
}

func (s *RedisStateStore) Get(ctx context.Context, key fwkdl.StateKey, endpointID string, aggregate func([]any) any) (any, bool, error) {
	pattern := s.scanPattern(key, endpointID)
	keys, err := s.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, false, fmt.Errorf("redis-state-store: keys: %w", err)
	}
	if len(keys) == 0 {
		return nil, false, nil
	}

	values := make([]any, 0, len(keys))
	for _, k := range keys {
		data, err := s.client.Get(ctx, k).Bytes()
		if err != nil {
			continue
		}
		val, err := gobDecode(data)
		if err != nil {
			continue
		}
		values = append(values, val)
	}
	if len(values) == 0 {
		return nil, false, nil
	}

	result := aggregate(values)
	logger := ctrl.LoggerFrom(ctx)
	if v := logger.V(logutil.DEBUG); v.Enabled() {
		v.Info("redis-state-store: Get", "key", string(key), "endpoint", endpointID, "replica", s.replicaID, "numKeys", len(keys), "result", fmt.Sprintf("%+v", result))
	}
	return result, true, nil
}

func (s *RedisStateStore) Delete(ctx context.Context, key fwkdl.StateKey, endpointID string) error {
	return s.client.Del(ctx, s.replicaKey(key, endpointID)).Err()
}
