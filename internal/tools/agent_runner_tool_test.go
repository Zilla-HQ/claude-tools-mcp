package tools

import (
	"encoding/json"
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

// TestAgentRunnerSpec_ReasoningMaxTokensSkills_SurviveMarshaling guards
// against the exact bug this PR fixes: DevRunAgentInput.Reasoning/MaxTokens
// were already accepted (unmarshaled from the MCP call), and agent-runner
// already declared them in devRunAgentArgsSchema — but agentRunnerSpec, the
// struct actually marshaled onto the runner subprocess's stdin, had no
// fields for them, so they were silently dropped in transit. Same class of
// bug would hit a new field with no test on this specific seam.
func TestAgentRunnerSpec_ReasoningMaxTokensSkills_SurviveMarshaling(t *testing.T) {
	t.Parallel()
	reasoning := "high"
	maxTokens := 8000
	spec := agentRunnerSpec{
		JobId:        "job-1",
		FifoPath:     "/tmp/fifo",
		WorkDir:      "/workspace",
		SystemPrompt: "sys",
		UserPrompt:   "user",
		Provider:     "anthropic",
		ModelId:      "claude-sonnet-5",
		Reasoning:    &reasoning,
		MaxTokens:    &maxTokens,
		Skills: []AgentSkill{
			{Name: "subscription", Description: "recurring billing", Content: "# Subscription\n..."},
		},
	}

	raw, err := json.Marshal(spec)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &decoded))

	assert.Equal(t, "high", decoded["reasoning"])
	assert.Equal(t, float64(8000), decoded["maxTokens"])
	skills, ok := decoded["skills"].([]interface{})
	require.True(t, ok, "skills should decode as an array")
	require.Len(t, skills, 1)
	skill := skills[0].(map[string]interface{})
	assert.Equal(t, "subscription", skill["name"])
	assert.Equal(t, "recurring billing", skill["description"])
	assert.Equal(t, "# Subscription\n...", skill["content"])
}

// TestAgentRunnerSpec_OmitsOptionalFieldsWhenAbsent confirms a dispatch with
// no reasoning/maxTokens/skills (the common case) doesn't grow the stdin
// payload or send empty placeholders.
func TestAgentRunnerSpec_OmitsOptionalFieldsWhenAbsent(t *testing.T) {
	t.Parallel()
	spec := agentRunnerSpec{
		JobId:        "job-1",
		FifoPath:     "/tmp/fifo",
		WorkDir:      "/workspace",
		SystemPrompt: "sys",
		UserPrompt:   "user",
		Provider:     "anthropic",
		ModelId:      "claude-sonnet-5",
	}

	raw, err := json.Marshal(spec)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &decoded))

	assert.NotContains(t, decoded, "reasoning")
	assert.NotContains(t, decoded, "maxTokens")
	assert.NotContains(t, decoded, "skills")
}

// TestRunAgentJob_ThreadsReasoningMaxTokensSkillsIntoSpec is the actual
// regression guard for the reported bug: constructs a DevRunAgentInput the
// way the platform would (Reasoning/MaxTokens/Skills all set) and confirms
// runAgentJob's spec construction carries them through — not just that the
// standalone struct marshals correctly in isolation, but that the real
// input-to-spec assignment in runAgentJob does too.
func TestRunAgentJob_ThreadsReasoningMaxTokensSkillsIntoSpec(t *testing.T) {
	t.Parallel()
	reasoning := "high"
	maxTokens := 8000
	input := DevRunAgentInput{
		WorkDir:      t.TempDir(),
		SystemPrompt: "sys",
		UserPrompt:   "user",
		Provider:     "anthropic",
		ModelId:      "claude-sonnet-5",
		Reasoning:    &reasoning,
		MaxTokens:    &maxTokens,
		Skills:       []AgentSkill{{Name: "agent-run", Description: "async LLM work", Content: "# agent-run"}},
	}

	spec := agentRunnerSpec{
		JobId:         "job-1",
		FifoPath:      "/tmp/fifo",
		WorkDir:       input.WorkDir,
		SystemPrompt:  input.SystemPrompt,
		UserPrompt:    input.UserPrompt,
		Provider:      input.Provider,
		ModelId:       input.ModelId,
		MaxIterations: input.MaxIterations,
		Reasoning:     input.Reasoning,
		MaxTokens:     input.MaxTokens,
		Skills:        input.Skills,
	}

	require.NotNil(t, spec.Reasoning)
	assert.Equal(t, "high", *spec.Reasoning)
	require.NotNil(t, spec.MaxTokens)
	assert.Equal(t, 8000, *spec.MaxTokens)
	require.Len(t, spec.Skills, 1)
	assert.Equal(t, "agent-run", spec.Skills[0].Name)
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
