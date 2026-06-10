package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mathematic-inc/claude-tools-mcp/internal/tools"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AsyncToolRequest represents the JSON request to POST /tools/call/async
type AsyncToolRequest struct {
	Tool      string                 `json:"tool"`
	Arguments map[string]interface{} `json:"arguments"`
}

// AsyncJobResponse represents the JSON response from POST /tools/call/async
type AsyncJobResponse struct {
	JobID string `json:"jobId"`
}

// JobStatusResponse represents the JSON response from GET /jobs/{jobId}
type JobStatusResponse struct {
	Status string      `json:"status"`
	Result interface{} `json:"result"`
	Error  *string     `json:"error"`
}

// setupTestServer creates an httptest.Server with the real async handlers wired to
// the provided state, allowing tests to use isolated state without touching the global.
func setupTestServer(t *testing.T, state *tools.State) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tools/call/async", handleAsyncStart(state))
	mux.HandleFunc("GET /jobs/{jobId}", handleGetJob(state))

	return httptest.NewServer(mux)
}

func TestAsyncStartJob_BashCommand(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	reqBody := AsyncToolRequest{
		Tool: "bash",
		Arguments: map[string]interface{}{
			"command": "echo hi",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusAccepted, resp.StatusCode)

	var jobResp AsyncJobResponse
	err = json.NewDecoder(resp.Body).Decode(&jobResp)
	require.NoError(t, err)

	assert.NotEmpty(t, jobResp.JobID)
}

func TestAsyncStartJob_UnknownTool(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	reqBody := AsyncToolRequest{
		Tool: "unknown_tool",
		Arguments: map[string]interface{}{
			"command": "echo hi",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAsyncStartJob_MalformedBody(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		strings.NewReader(`{invalid json}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAsyncGetJob_RunningStatus(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	// First, POST to start a job
	reqBody := AsyncToolRequest{
		Tool: "bash",
		Arguments: map[string]interface{}{
			"command": "sleep 2",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	var jobResp AsyncJobResponse
	err = json.NewDecoder(resp.Body).Decode(&jobResp)
	require.NoError(t, err)

	jobID := jobResp.JobID

	// Immediately GET the job status
	getResp, err := http.Get(server.URL + "/jobs/" + jobID)
	require.NoError(t, err)
	defer getResp.Body.Close()

	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	var statusResp JobStatusResponse
	err = json.NewDecoder(getResp.Body).Decode(&statusResp)
	require.NoError(t, err)

	assert.Equal(t, "running", statusResp.Status)
	assert.Nil(t, statusResp.Error)
}

func TestAsyncGetJob_AfterCompletion(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	// Start a quick job
	reqBody := AsyncToolRequest{
		Tool: "bash",
		Arguments: map[string]interface{}{
			"command": "echo success",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	var jobResp AsyncJobResponse
	err = json.NewDecoder(resp.Body).Decode(&jobResp)
	require.NoError(t, err)

	jobID := jobResp.JobID

	// Poll for completion
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		getResp, err := http.Get(server.URL + "/jobs/" + jobID)
		require.NoError(t, err)

		var statusResp JobStatusResponse
		err = json.NewDecoder(getResp.Body).Decode(&statusResp)
		getResp.Body.Close()
		require.NoError(t, err)

		if statusResp.Status == "done" {
			assert.NotNil(t, statusResp.Result, "result should be set for completed job")
			assert.Nil(t, statusResp.Error)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("job should complete within timeout")
}

func TestAsyncGetJob_UnknownJobID(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	resp, err := http.Get(server.URL + "/jobs/job_unknown")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAsyncStartJob_ConcurrentRequests(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	const numRequests = 10
	jobIDs := make([]string, numRequests)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			reqBody := AsyncToolRequest{
				Tool: "bash",
				Arguments: map[string]interface{}{
					"command": fmt.Sprintf("echo %d", idx),
				},
			}
			bodyBytes, _ := json.Marshal(reqBody)

			resp, err := http.Post(
				server.URL+"/tools/call/async",
				"application/json",
				bytes.NewReader(bodyBytes),
			)
			require.NoError(t, err)
			defer resp.Body.Close()

			var jobResp AsyncJobResponse
			err = json.NewDecoder(resp.Body).Decode(&jobResp)
			require.NoError(t, err)

			mu.Lock()
			jobIDs[idx] = jobResp.JobID
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify all IDs are unique
	seen := make(map[string]bool)
	for _, jobID := range jobIDs {
		assert.NotEmpty(t, jobID)
		assert.False(t, seen[jobID], "job ID %s should be unique", jobID)
		seen[jobID] = true
	}

	assert.Equal(t, numRequests, len(seen))
}

func TestAsyncPostRequest_StatusCode(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	reqBody := AsyncToolRequest{
		Tool: "bash",
		Arguments: map[string]interface{}{
			"command": "echo test",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should return 202 Accepted on success
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestAsyncGetJob_ContentType(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	// Start a job
	reqBody := AsyncToolRequest{
		Tool: "bash",
		Arguments: map[string]interface{}{
			"command": "sleep 1",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	var jobResp AsyncJobResponse
	err = json.NewDecoder(resp.Body).Decode(&jobResp)
	require.NoError(t, err)

	jobID := jobResp.JobID

	// GET job status
	getResp, err := http.Get(server.URL + "/jobs/" + jobID)
	require.NoError(t, err)
	defer getResp.Body.Close()

	// Verify response is JSON
	assert.Contains(t, getResp.Header.Get("Content-Type"), "application/json")
}

func TestAsyncPostRequest_EmptyArguments(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	reqBody := AsyncToolRequest{
		Tool:      "bash",
		Arguments: map[string]interface{}{},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should either accept or reject based on implementation
	// For now, just verify we get a valid response
	assert.NotEqual(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestAsyncGetJob_FailedJob(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	// Start a job that fails
	reqBody := AsyncToolRequest{
		Tool: "bash",
		Arguments: map[string]interface{}{
			"command": "exit 1",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	var jobResp AsyncJobResponse
	err = json.NewDecoder(resp.Body).Decode(&jobResp)
	require.NoError(t, err)

	jobID := jobResp.JobID

	// Poll for completion
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		getResp, err := http.Get(server.URL + "/jobs/" + jobID)
		require.NoError(t, err)

		var statusResp JobStatusResponse
		body, _ := io.ReadAll(getResp.Body)
		getResp.Body.Close()

		err = json.Unmarshal(body, &statusResp)
		require.NoError(t, err)

		if statusResp.Status == "failed" {
			assert.NotNil(t, statusResp.Error, "error should be set for failed job")
			assert.NotEmpty(t, *statusResp.Error)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("job should fail within timeout")
}

func TestAsyncGetJob_SequentialIDs(t *testing.T) {
	t.Parallel()
	state := tools.NewState()
	defer state.StopEviction()
	server := setupTestServer(t, state)
	defer server.Close()

	reqBody1 := AsyncToolRequest{
		Tool: "bash",
		Arguments: map[string]interface{}{
			"command": "echo 1",
		},
	}
	bodyBytes1, _ := json.Marshal(reqBody1)

	resp1, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes1),
	)
	require.NoError(t, err)
	defer resp1.Body.Close()

	var jobResp1 AsyncJobResponse
	err = json.NewDecoder(resp1.Body).Decode(&jobResp1)
	require.NoError(t, err)

	reqBody2 := AsyncToolRequest{
		Tool: "bash",
		Arguments: map[string]interface{}{
			"command": "echo 2",
		},
	}
	bodyBytes2, _ := json.Marshal(reqBody2)

	resp2, err := http.Post(
		server.URL+"/tools/call/async",
		"application/json",
		bytes.NewReader(bodyBytes2),
	)
	require.NoError(t, err)
	defer resp2.Body.Close()

	var jobResp2 AsyncJobResponse
	err = json.NewDecoder(resp2.Body).Decode(&jobResp2)
	require.NoError(t, err)

	assert.NotEmpty(t, jobResp1.JobID)
	assert.NotEmpty(t, jobResp2.JobID)
	assert.NotEqual(t, jobResp1.JobID, jobResp2.JobID)
}
