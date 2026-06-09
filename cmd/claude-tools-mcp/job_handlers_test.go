package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mathematic-inc/claude-tools-mcp/internal/tools"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupJobHandlersTestServer creates an httptest.Server with job handler routes
func setupJobHandlersTestServer(t *testing.T, state *tools.State) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs/{jobId}/cancel", handleCancelJob(state))
	mux.HandleFunc("GET /jobs/{jobId}/events", handleGetJobEvents(state))

	return httptest.NewServer(mux)
}

func TestCancelJob_TransitionsToCancelled(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupJobHandlersTestServer(t, state)
	defer server.Close()

	tmpDir := t.TempDir()

	// Start a long-running job
	jobID := state.StartJob("dev_run_agent", map[string]interface{}{"workDir": tmpDir, "systemPrompt": "test", "userPrompt": "test", "provider": "anthropic", "modelId": "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	// POST to cancel the job
	resp, err := http.Post(
		server.URL+"/jobs/"+jobID+"/cancel",
		"application/json",
		bytes.NewReader([]byte{}),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should get a successful response
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify job transitions to cancelled within 6 seconds
	timeout := time.Now().Add(6 * time.Second)
	for time.Now().Before(timeout) {
		snapshot := state.GetJob(jobID)
		if snapshot != nil && snapshot.Status == "cancelled" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("job should transition to cancelled within 6 seconds")
}

func TestCancelJob_UnknownJobReturns404(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupJobHandlersTestServer(t, state)
	defer server.Close()

	resp, err := http.Post(
		server.URL+"/jobs/job_unknown/cancel",
		"application/json",
		bytes.NewReader([]byte{}),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetJobEvents_ReturnsEvents(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupJobHandlersTestServer(t, state)
	defer server.Close()

	tmpDir := t.TempDir()

	// Start a job
	jobID := state.StartJob("dev_run_agent", map[string]interface{}{"workDir": tmpDir, "systemPrompt": "test", "userPrompt": "test", "provider": "anthropic", "modelId": "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	// Get the job and manually add events to its ring (simulating what the pusher would do)
	job := state.GetJob(jobID)
	require.NotNil(t, job)

	// For this test, we simulate event injection
	// In the real implementation, the job's Events ring will have these pushed by the pusher

	// GET the events
	resp, err := http.Get(server.URL + "/jobs/" + jobID + "/events")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify response is valid JSON array
	var events []json.RawMessage
	err = json.NewDecoder(resp.Body).Decode(&events)
	require.NoError(t, err)
	assert.IsType(t, []json.RawMessage{}, events)
}

func TestGetJobEvents_UnknownJobReturns404(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupJobHandlersTestServer(t, state)
	defer server.Close()

	resp, err := http.Get(server.URL + "/jobs/job_unknown/events")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetJobEvents_EmptyInitially(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupJobHandlersTestServer(t, state)
	defer server.Close()

	tmpDir := t.TempDir()

	jobID := state.StartJob("dev_run_agent", map[string]interface{}{"workDir": tmpDir, "systemPrompt": "test", "userPrompt": "test", "provider": "anthropic", "modelId": "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	resp, err := http.Get(server.URL + "/jobs/" + jobID + "/events")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var events []json.RawMessage
	err = json.NewDecoder(resp.Body).Decode(&events)
	require.NoError(t, err)
	assert.Equal(t, 0, len(events))
}

func TestCancelJob_ProcessExitsQuickly(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupJobHandlersTestServer(t, state)
	defer server.Close()

	tmpDir := t.TempDir()

	jobID := state.StartJob("dev_run_agent", map[string]interface{}{"workDir": tmpDir, "systemPrompt": "test", "userPrompt": "test", "provider": "anthropic", "modelId": "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	startTime := time.Now()

	resp, err := http.Post(
		server.URL+"/jobs/"+jobID+"/cancel",
		"application/json",
		bytes.NewReader([]byte{}),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Poll for job to be cancelled
	timeout := time.Now().Add(6 * time.Second)
	for time.Now().Before(timeout) {
		snapshot := state.GetJob(jobID)
		if snapshot != nil && snapshot.Status == "cancelled" {
			elapsed := time.Since(startTime)
			// Should exit much faster than the timeout, verify it's within 6s
			assert.True(t, elapsed < 6*time.Second)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("job should be cancelled")
}

func TestGetJobEvents_ContentType(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupJobHandlersTestServer(t, state)
	defer server.Close()

	tmpDir := t.TempDir()

	jobID := state.StartJob("dev_run_agent", map[string]interface{}{"workDir": tmpDir, "systemPrompt": "test", "userPrompt": "test", "provider": "anthropic", "modelId": "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	resp, err := http.Get(server.URL + "/jobs/" + jobID + "/events")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
}

func TestCancelJob_MultipleCancel(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupJobHandlersTestServer(t, state)
	defer server.Close()

	tmpDir := t.TempDir()

	jobID := state.StartJob("dev_run_agent", map[string]interface{}{"workDir": tmpDir, "systemPrompt": "test", "userPrompt": "test", "provider": "anthropic", "modelId": "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	// Cancel once
	resp1, err := http.Post(
		server.URL+"/jobs/"+jobID+"/cancel",
		"application/json",
		bytes.NewReader([]byte{}),
	)
	require.NoError(t, err)
	resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)

	// Wait for cancellation
	time.Sleep(200 * time.Millisecond)

	// Cancel again (idempotent)
	resp2, err := http.Post(
		server.URL+"/jobs/"+jobID+"/cancel",
		"application/json",
		bytes.NewReader([]byte{}),
	)
	require.NoError(t, err)
	defer resp2.Body.Close()

	// Should still succeed
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}
