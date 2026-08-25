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

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/llm-d/llm-d-router/pkg/kvevents"
)

const KVEventCheckpointPath = "/checkpoint"

const checkpointShutdownTimeout = 5 * time.Second

const (
	checkpointStatusRunning   = "running"
	checkpointStatusSucceeded = "succeeded"
	checkpointStatusFailed    = "failed"
)

type kvEventCheckpointResponse struct {
	ID         string                     `json:"id"`
	Status     string                     `json:"status"`
	StartedAt  *time.Time                 `json:"startedAt,omitempty"`
	FinishedAt *time.Time                 `json:"finishedAt,omitempty"`
	Checkpoint *kvevents.CheckpointResult `json:"checkpoint,omitempty"`
	Error      string                     `json:"error,omitempty"`
}

// KVEventCheckpointFunc writes the precise-prefix state derived from KV events.
type KVEventCheckpointFunc func() (kvevents.CheckpointResult, error)

func NewKVEventCheckpointHandler(writeCheckpoint KVEventCheckpointFunc) http.Handler {
	return &kvEventCheckpointHandler{
		writeCheckpoint: writeCheckpoint,
		operations:      make(map[string]kvEventCheckpointResponse),
	}
}

type kvEventCheckpointHandler struct {
	writeCheckpoint KVEventCheckpointFunc
	mu              sync.RWMutex
	operations      map[string]kvEventCheckpointResponse
	runningID       string
}

func (h *kvEventCheckpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == KVEventCheckpointPath {
		if r.Method == http.MethodPost {
			h.start(w)
			return
		}
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.HasPrefix(r.URL.Path, KVEventCheckpointPath+"/") {
		if r.Method == http.MethodGet {
			h.get(w, strings.TrimPrefix(r.URL.Path, KVEventCheckpointPath+"/"))
			return
		}
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.NotFound(w, r)
}

func (h *kvEventCheckpointHandler) start(w http.ResponseWriter) {
	if h.writeCheckpoint == nil {
		http.Error(w, "KV-event checkpoint writer is not configured", http.StatusInternalServerError)
		return
	}

	h.mu.Lock()
	if h.runningID != "" {
		operation := h.operations[h.runningID]
		h.mu.Unlock()
		w.Header().Set("Location", KVEventCheckpointPath+"/"+operation.ID)
		h.writeResponse(w, http.StatusConflict, operation)
		return
	}
	startedAt := nowFunc().UTC()
	operation := kvEventCheckpointResponse{
		ID: uuid.NewString(), Status: checkpointStatusRunning, StartedAt: &startedAt,
	}
	h.operations[operation.ID] = operation
	h.runningID = operation.ID
	h.mu.Unlock()
	go h.run(operation.ID)
	w.Header().Set("Location", KVEventCheckpointPath+"/"+operation.ID)
	h.writeResponse(w, http.StatusAccepted, operation)
}

func (h *kvEventCheckpointHandler) run(operationID string) {
	result, checkpointErr := h.writeCheckpoint()
	finishedAt := nowFunc().UTC()
	h.mu.Lock()
	defer h.mu.Unlock()
	operation := h.operations[operationID]
	operation.FinishedAt = &finishedAt
	if checkpointErr != nil {
		operation.Status = checkpointStatusFailed
		operation.Error = fmt.Errorf("write KV-event checkpoint: %w", checkpointErr).Error()
	} else {
		operation.Status = checkpointStatusSucceeded
		operation.Checkpoint = &result
	}
	h.operations[operationID] = operation
	h.runningID = ""
}

func (h *kvEventCheckpointHandler) get(w http.ResponseWriter, operationID string) {
	h.mu.RLock()
	operation, ok := h.operations[operationID]
	h.mu.RUnlock()
	if !ok || operationID == "" || strings.Contains(operationID, "/") {
		http.Error(w, "checkpoint operation not found", http.StatusNotFound)
		return
	}
	h.writeResponse(w, http.StatusOK, operation)
}

func (h *kvEventCheckpointHandler) writeResponse(w http.ResponseWriter, statusCode int, operation kvEventCheckpointResponse) {
	payload, err := json.Marshal(operation)
	if err != nil {
		http.Error(w, fmt.Sprintf("encode checkpoint response: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(payload)
}

// ServeKVEventCheckpoint starts the dedicated checkpoint-write HTTP server.
func ServeKVEventCheckpoint(ctx context.Context, port int, writeCheckpoint KVEventCheckpointFunc) error {
	mux := http.NewServeMux()
	handler := NewKVEventCheckpointHandler(writeCheckpoint)
	mux.Handle(KVEventCheckpointPath, handler)
	mux.Handle(KVEventCheckpointPath+"/", handler)
	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), checkpointShutdownTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("checkpoint server: %w", err)
	}
	return nil
}
