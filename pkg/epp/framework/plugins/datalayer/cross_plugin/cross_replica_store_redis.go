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
	"time"

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

const (
	RedisStateStoreType = "redis-state-store"
	defaultTTL          = 180 * time.Second
)

var _ fwkdl.CrossReplicaStore = (*RedisStateStore)(nil)

type redisConfig struct {
	Address  string `json:"address"`
	Password string `json:"password"`
	DB       int    `json:"db"`
	TTL      string `json:"ttl"`
}

// RedisStateStore is a CrossReplicaStore backed by Redis for cross-replica
// state sharing. Each endpoint's state is stored in a Redis hash keyed by
// "{stateKey}:{endpointID}", with per-replica fields keyed by replicaID.
// Get aggregates all replica fields using the caller-supplied aggregate
// function via a single HGETALL.
type RedisStateStore struct {
	typedName fwkplugin.TypedName
	replicaID string
	client    *redis.Client
	ttl       time.Duration
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

	ttl := defaultTTL
	if cfg.TTL != "" {
		parsed, err := time.ParseDuration(cfg.TTL)
		if err != nil {
			return nil, fmt.Errorf("redis-state-store: invalid ttl %q: %w", cfg.TTL, err)
		}
		ttl = parsed
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

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis-state-store: failed to connect to Redis at %s: %w", cfg.Address, err)
	}

	return &RedisStateStore{
		typedName: fwkplugin.TypedName{Type: RedisStateStoreType, Name: name},
		replicaID: hostname,
		client:    client,
		ttl:       ttl,
	}, nil
}

func (s *RedisStateStore) TypedName() fwkplugin.TypedName {
	return s.typedName
}

func (s *RedisStateStore) hashKey(key fwkdl.StateKey, endpointID string) string {
	return string(key) + ":" + endpointID
}

type stampedValue struct {
	Value   any
	WrittenAt time.Time
}

func gobEncode(value any, now time.Time) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(stampedValue{Value: value, WrittenAt: now}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(data []byte) (any, time.Time, error) {
	var w stampedValue
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return nil, time.Time{}, err
	}
	return w.Value, w.WrittenAt, nil
}

func (s *RedisStateStore) Set(ctx context.Context, key fwkdl.StateKey, endpointID string, value any) error {
	logger := ctrl.LoggerFrom(ctx)
	data, err := gobEncode(value, time.Now())
	if err != nil {
		return fmt.Errorf("redis-state-store: encode: %w", err)
	}
	hk := s.hashKey(key, endpointID)
	pipe := s.client.Pipeline()
	pipe.HSet(ctx, hk, s.replicaID, data)
	pipe.Expire(ctx, hk, s.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	if v := logger.V(logutil.DEBUG); v.Enabled() {
		v.Info("redis-state-store: Set", "key", string(key), "endpoint", endpointID, "replica", s.replicaID, "value", fmt.Sprintf("%+v", value))
	}
	return nil
}

func (s *RedisStateStore) Get(ctx context.Context, key fwkdl.StateKey, endpointID string, aggregate func([]any) any) (any, bool, error) {
	hk := s.hashKey(key, endpointID)
	result, err := s.client.HGetAll(ctx, hk).Result()
	if err != nil {
		return nil, false, fmt.Errorf("redis-state-store: hgetall: %w", err)
	}
	if len(result) == 0 {
		return nil, false, nil
	}

	logger := ctrl.LoggerFrom(ctx)
	now := time.Now()
	values := make([]any, 0, len(result))
	for field, data := range result {
		val, writtenAt, err := gobDecode([]byte(data))
		if err != nil {
			if v := logger.V(logutil.DEBUG); v.Enabled() {
				v.Info("redis-state-store: decode error", "field", field, "error", err)
			}
			continue
		}
		if now.Sub(writtenAt) > s.ttl {
			if v := logger.V(logutil.DEBUG); v.Enabled() {
				v.Info("redis-state-store: skipping stale entry", "field", field, "age", now.Sub(writtenAt), "ttl", s.ttl)
			}
			continue
		}
		values = append(values, val)
	}
	if len(values) == 0 {
		return nil, false, nil
	}

	aggregated := aggregate(values)
	if v := logger.V(logutil.DEBUG); v.Enabled() {
		v.Info("redis-state-store: Get", "key", string(key), "endpoint", endpointID, "replica", s.replicaID, "numReplicas", len(values), "result", fmt.Sprintf("%+v", aggregated))
	}
	return aggregated, true, nil
}

func (s *RedisStateStore) Delete(ctx context.Context, key fwkdl.StateKey, endpointID string) error {
	return s.client.HDel(ctx, s.hashKey(key, endpointID), s.replicaID).Err()
}
