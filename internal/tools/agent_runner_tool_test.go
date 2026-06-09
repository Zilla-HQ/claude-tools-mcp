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
	assert.Equal(t, "dev_run_agent", DevRunAgentTool.Name)
}

func startAgentJob(state *State, input DevRunAgentInput) string {
	return state.StartJob("dev_run_agent", map[string]interface{}{
		"workDir":      input.WorkDir,
		"systemPrompt": input.SystemPrompt,
		"userPrompt":   input.UserPrompt,
		"provider":     input.Provider,
		"modelId":      input.ModelId,
	})
}

func TestStartJob_DevRunAgent_ReturnsJobID(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	tmpDir := t.TempDir()

	jobID := startAgentJob(state, DevRunAgentInput{WorkDir: tmpDir, SystemPrompt: "test", UserPrompt: "test", Provider: "anthropic", ModelId: "claude-haiku-4-5-20251001"})

	assert.NotEmpty(t, jobID)
}

func TestStartJob_DevRunAgent_JobAppearsInState(t *testing.T) {
	t.Parallel()
	state := NewState()
	defer state.StopEviction()

	tmpDir := t.TempDir()

	jobID := startAgentJob(state, DevRunAgentInput{WorkDir: tmpDir, SystemPrompt: "test", UserPrompt: "test", Provider: "anthropic", ModelId: "claude-haiku-4-5-20251001"})

	snapshot := state.GetJob(jobID)
	require.NotNil(t, snapshot, "job should exist in state")
	assert.Equal(t, "running", snapshot.Status)
}

func TestStartJob_DevRunAgent_CreatesFIFO(t *testing.T) {
	state := NewState()
	defer state.StopEviction()

	tmpDir := t.TempDir()
	runtimeDir := filepath.Join(tmpDir, "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o755))

	t.Setenv("ZILLA_JOB_RUNTIME_DIR", runtimeDir)

	jobID := startAgentJob(state, DevRunAgentInput{WorkDir: tmpDir, SystemPrompt: "test", UserPrompt: "test", Provider: "anthropic", ModelId: "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	time.Sleep(100 * time.Millisecond)

	fifoPath := filepath.Join(runtimeDir, jobID, "events.fifo")
	_, err := os.Stat(fifoPath)
	assert.NoError(t, err, "FIFO should be created at %s", fifoPath)
}

func TestStartJob_DevRunAgent_MissingAgentRunner(t *testing.T) {
	state := NewState()
	defer state.StopEviction()

	tmpDir := t.TempDir()
	t.Setenv("ZILLA_AGENT_RUNNER_PATH", "")

	jobID := startAgentJob(state, DevRunAgentInput{WorkDir: tmpDir, SystemPrompt: "test", UserPrompt: "test", Provider: "anthropic", ModelId: "claude-haiku-4-5-20251001"})
	assert.NotEmpty(t, jobID)

	time.Sleep(100 * time.Millisecond)

	snapshot := state.GetJob(jobID)
	require.NotNil(t, snapshot)
	assert.NotEmpty(t, snapshot.Status)
}
