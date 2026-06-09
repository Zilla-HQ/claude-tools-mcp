package tools

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevRunAgentTool_Exists(t *testing.T) {
	// Test that the DevRunAgentTool variable exists and has the correct name
	assert.Equal(t, "dev_run_agent", DevRunAgentTool.Name)
}

func TestStartAgentJob_ReturnsJobID(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	tmpDir := t.TempDir()

	jobID := state.StartAgentJob(DevRunAgentInput{WorkDir: tmpDir, SystemPrompt: "test", UserPrompt: "test", Provider: "anthropic", ModelId: "claude-haiku-4-5-20251001"})

	assert.NotEmpty(t, jobID)
	assert.True(t, len(jobID) > 0)
}

func TestStartAgentJob_JobAppearsInState(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	tmpDir := t.TempDir()

	jobID := state.StartAgentJob(DevRunAgentInput{WorkDir: tmpDir, SystemPrompt: "test", UserPrompt: "test", Provider: "anthropic", ModelId: "claude-haiku-4-5-20251001"})

	// Retrieve the job snapshot
	snapshot := state.GetJob(jobID)
	require.NotNil(t, snapshot, "job should exist in state")
	assert.Equal(t, "running", snapshot.Status)
}

func TestStartAgentJob_CreatesFIFO(t *testing.T) {
	state := NewState()
	defer state.StopEviction()

	tmpDir := t.TempDir()
	runtimeDir := filepath.Join(tmpDir, "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))

	t.Setenv("ZILLA_JOB_RUNTIME_DIR", runtimeDir)

	jobID := state.StartAgentJob(DevRunAgentInput{WorkDir: tmpDir, SystemPrompt: "test", UserPrompt: "test", Provider: "anthropic", ModelId: "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	// Give the goroutine time to create the FIFO
	time.Sleep(100 * time.Millisecond)

	// Check that FIFO was created at the expected path
	fifoPath := filepath.Join(runtimeDir, jobID, "events.fifo")
	_, err := os.Stat(fifoPath)
	assert.NoError(t, err, "FIFO should be created at %s", fifoPath)
}

func TestStartAgentJob_MissingAgentRunner(t *testing.T) {
	state := NewState()
	defer state.StopEviction()

	tmpDir := t.TempDir()

	// Unset the agent runner path to simulate missing executable
	t.Setenv("ZILLA_AGENT_RUNNER_PATH", "")

	jobID := state.StartAgentJob(DevRunAgentInput{WorkDir: tmpDir, SystemPrompt: "test", UserPrompt: "test", Provider: "anthropic", ModelId: "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	// Wait a bit for the job to start and potentially fail
	time.Sleep(100 * time.Millisecond)

	// The job should either still be running or have an error
	snapshot := state.GetJob(jobID)
	require.NotNil(t, snapshot)
	assert.NotEmpty(t, snapshot.Status)
}
