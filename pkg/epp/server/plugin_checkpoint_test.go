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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/kvevents"
)

type checkpointTestWriter struct {
	result  kvevents.CheckpointResult
	err     error
	started chan struct{}
	release chan struct{}
}

func (w *checkpointTestWriter) WriteCheckpoint() (kvevents.CheckpointResult, error) {
	if w.started != nil {
		close(w.started)
	}
	if w.release != nil {
		<-w.release
	}
	return w.result, w.err
}

func TestKVEventCheckpointHandler(t *testing.T) {
	createdAt := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	writer := &checkpointTestWriter{
		result:  kvevents.CheckpointResult{Path: "/data/index", CreatedAt: createdAt, Sources: 2},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	handler := NewKVEventCheckpointHandler(writer.WriteCheckpoint)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, KVEventCheckpointPath, nil)
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusAccepted, recorder.Code)
	var response kvEventCheckpointResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.NotEmpty(t, response.ID)
	require.Equal(t, "running", response.Status)
	require.NotNil(t, response.StartedAt)
	require.Nil(t, response.Checkpoint)
	require.Equal(t, KVEventCheckpointPath+"/"+response.ID, recorder.Header().Get("Location"))
	<-writer.started

	statusRecorder := httptest.NewRecorder()
	handler.ServeHTTP(statusRecorder, httptest.NewRequest(http.MethodGet, recorder.Header().Get("Location"), nil))
	require.Equal(t, http.StatusOK, statusRecorder.Code)
	require.NoError(t, json.Unmarshal(statusRecorder.Body.Bytes(), &response))
	require.Equal(t, "running", response.Status)

	close(writer.release)
	require.Eventually(t, func() bool {
		statusRecorder = httptest.NewRecorder()
		handler.ServeHTTP(statusRecorder, httptest.NewRequest(http.MethodGet, KVEventCheckpointPath+"/"+response.ID, nil))
		return json.Unmarshal(statusRecorder.Body.Bytes(), &response) == nil && response.Status == checkpointStatusSucceeded
	}, time.Second, 10*time.Millisecond)
	require.NotNil(t, response.FinishedAt)
	require.Equal(t, writer.result, *response.Checkpoint)

	handler = NewKVEventCheckpointHandler((&checkpointTestWriter{}).WriteCheckpoint)
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodPost, KVEventCheckpointPath, nil))
	require.Equal(t, http.StatusAccepted, second.Code)
	var secondResponse kvEventCheckpointResponse
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &secondResponse))
	require.NotEqual(t, response.ID, secondResponse.ID)
	require.Eventually(t, func() bool {
		status := httptest.NewRecorder()
		handler.ServeHTTP(status, httptest.NewRequest(http.MethodGet, second.Header().Get("Location"), nil))
		return json.Unmarshal(status.Body.Bytes(), &secondResponse) == nil && secondResponse.Status == checkpointStatusSucceeded
	}, time.Second, 10*time.Millisecond)
}

func TestKVEventCheckpointHandlerRejectsConcurrentRequest(t *testing.T) {
	writer := &checkpointTestWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	handler := NewKVEventCheckpointHandler(writer.WriteCheckpoint)

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodPost, KVEventCheckpointPath, nil))
	require.Equal(t, http.StatusAccepted, first.Code)
	<-writer.started

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodPost, KVEventCheckpointPath, nil))
	require.Equal(t, http.StatusConflict, second.Code)
	require.Equal(t, first.Header().Get("Location"), second.Header().Get("Location"))
	var response kvEventCheckpointResponse
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &response))
	require.Equal(t, "running", response.Status)

	close(writer.release)
	require.Eventually(t, func() bool {
		status := httptest.NewRecorder()
		handler.ServeHTTP(status, httptest.NewRequest(http.MethodGet, first.Header().Get("Location"), nil))
		return json.Unmarshal(status.Body.Bytes(), &response) == nil && response.Status == checkpointStatusSucceeded
	}, time.Second, 10*time.Millisecond)
}

func TestKVEventCheckpointHandlerReturnsNotFoundForUnknownOperation(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, KVEventCheckpointPath+"/unknown", nil)
	NewKVEventCheckpointHandler((&checkpointTestWriter{}).WriteCheckpoint).ServeHTTP(recorder, request)
	require.Equal(t, http.StatusNotFound, recorder.Code)
}

func TestKVEventCheckpointHandlerRejectsUnsupportedMethod(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, KVEventCheckpointPath, nil)
	NewKVEventCheckpointHandler((&checkpointTestWriter{}).WriteCheckpoint).ServeHTTP(recorder, request)
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	require.Equal(t, "POST", recorder.Header().Get("Allow"))
}
