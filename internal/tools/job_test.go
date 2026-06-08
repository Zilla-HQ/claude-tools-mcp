package tools

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartJob_ReturnsSequentialIDs(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	// Start first job
	jobID1 := state.StartJob("bash", map[string]interface{}{"command": "echo test"})
	assert.Equal(t, "job_1", jobID1)

	// Start second job
	jobID2 := state.StartJob("bash", map[string]interface{}{"command": "echo test2"})
	assert.Equal(t, "job_2", jobID2)

	// Start third job
	jobID3 := state.StartJob("bash", map[string]interface{}{"command": "echo test3"})
	assert.Equal(t, "job_3", jobID3)
}

func TestGetJob_WhileRunning(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	// Start a job
	jobID := state.StartJob("bash", map[string]interface{}{"command": "sleep 2"})

	// Immediately check status
	job := state.GetJob(jobID)
	require.NotNil(t, job)
	assert.Equal(t, "running", job.Status)
	assert.Nil(t, job.Result)
	assert.Nil(t, job.Error)
}

func TestGetJob_AfterCompletion(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	// Start a job that completes quickly
	jobID := state.StartJob("bash", map[string]interface{}{"command": "echo success"})

	// Wait for job to complete (with timeout)
	done := make(chan bool, 1)
	go func() {
		for i := 0; i < 50; i++ {
			job := state.GetJob(jobID)
			if job != nil && job.Status == "done" {
				done <- true
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		done <- false
	}()

	success := <-done
	require.True(t, success, "job should complete within timeout")

	// Verify final state
	job := state.GetJob(jobID)
	require.NotNil(t, job)
	assert.Equal(t, "done", job.Status)
	assert.NotNil(t, job.Result, "result should be set for completed job")
	assert.Nil(t, job.Error, "error should be nil for successful job")
}

func TestGetJob_AfterFailure(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	// Start a job that fails
	jobID := state.StartJob("bash", map[string]interface{}{"command": "exit 1"})

	// Wait for job to complete (with timeout)
	done := make(chan bool, 1)
	go func() {
		for i := 0; i < 50; i++ {
			job := state.GetJob(jobID)
			if job != nil && job.Status == "failed" {
				done <- true
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		done <- false
	}()

	success := <-done
	require.True(t, success, "job should fail within timeout")

	// Verify final state
	job := state.GetJob(jobID)
	require.NotNil(t, job)
	assert.Equal(t, "failed", job.Status)
	assert.NotNil(t, job.Error, "error should be set for failed job")
	assert.NotEmpty(t, *job.Error, "error message should be non-empty")
}

func TestGetJob_UnknownID(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	job := state.GetJob("job_999")
	assert.Nil(t, job)
}

func TestStartJob_ConcurrentIDsAreUnique(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	const numGoroutines = 10
	jobIDs := make([]string, numGoroutines)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			jobID := state.StartJob("bash", map[string]interface{}{"command": "sleep 1"})
			mu.Lock()
			jobIDs[idx] = jobID
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

	// Verify we have expected number of unique IDs
	assert.Equal(t, numGoroutines, len(seen))
}

func TestJob_StructFields(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	jobID := state.StartJob("bash", map[string]interface{}{"command": "echo test"})
	job := state.GetJob(jobID)

	require.NotNil(t, job)
	assert.NotEmpty(t, job.ID)
	assert.Equal(t, "running", job.Status)

	// Should have structured fields for tool name and arguments
	assert.NotEmpty(t, job.ToolName)
	assert.NotEmpty(t, job.Arguments)
}

// Helper test to verify Job struct exists and has expected methods
func TestJob_CreatedTime(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	before := time.Now()
	jobID := state.StartJob("bash", map[string]interface{}{"command": "sleep 1"})
	after := time.Now()

	job := state.GetJob(jobID)
	require.NotNil(t, job)
	assert.True(t, job.CreatedAt.After(before) || job.CreatedAt.Equal(before),
		"job CreatedAt should be after or equal to before time")
	assert.True(t, job.CreatedAt.Before(after) || job.CreatedAt.Equal(after),
		"job CreatedAt should be before or equal to after time")
}

func TestGetJob_ReturnsConsistentResults(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	jobID := state.StartJob("bash", map[string]interface{}{"command": "echo test"})

	// Call GetJob multiple times and verify consistent results while running
	job1 := state.GetJob(jobID)
	job2 := state.GetJob(jobID)

	require.NotNil(t, job1)
	require.NotNil(t, job2)
	assert.Equal(t, job1.ID, job2.ID)
	assert.Equal(t, job1.Status, job2.Status)
}

// Test to verify that Job struct can hold the SDK result
func TestJob_StoresCallToolResult(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	// Start a job that will complete
	jobID := state.StartJob("bash", map[string]interface{}{"command": "echo hello"})

	// Wait for completion
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		job := state.GetJob(jobID)
		if job != nil && job.Status == "done" && job.Result != nil {
			// Result is typed as *sdk.CallToolResult in JobSnapshot
			assert.NotNil(t, job.Result, "result should be a CallToolResult")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("job should complete with result within timeout")
}
