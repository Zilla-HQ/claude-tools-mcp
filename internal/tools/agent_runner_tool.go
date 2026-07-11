package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

var DevRunAgentTool = sdk.Tool{
	Name:        "dev_run_agent",
	Description: "Spawn the agent-runner process for a given working directory. Returns a jobId.",
}

type DevRunAgentInput struct {
	WorkDir       string `json:"workDir"`
	SystemPrompt  string `json:"systemPrompt"`
	UserPrompt    string `json:"userPrompt"`
	Provider      string `json:"provider"`
	ModelId       string `json:"modelId"`
	MaxIterations *int   `json:"maxIterations,omitempty"`
}

// agentRunnerSpec is the JSON blob written to the runner subprocess's stdin.
type agentRunnerSpec struct {
	JobId         string `json:"jobId"`
	FifoPath      string `json:"fifoPath"`
	WorkDir       string `json:"workDir"`
	SystemPrompt  string `json:"systemPrompt"`
	UserPrompt    string `json:"userPrompt"`
	Provider      string `json:"provider"`
	ModelId       string `json:"modelId"`
	MaxIterations *int   `json:"maxIterations,omitempty"`
}

func (s *State) runAgentJob(job *Job, input DevRunAgentInput) {
	workDir := input.WorkDir
	defer job.cancel()

	runtimeDir := os.Getenv("ZILLA_JOB_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = os.TempDir()
	}

	jobDir := filepath.Join(runtimeDir, job.ID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		s.failJob(job, fmt.Sprintf("failed to create job dir: %v", err))
		log.Printf("[agent-runner] job %s: failed to create job dir: %v", job.ID, err)
		return
	}

	fifoPath := filepath.Join(jobDir, "events.fifo")
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		s.failJob(job, fmt.Sprintf("failed to create FIFO: %v", err))
		log.Printf("[agent-runner] job %s: failed to create FIFO: %v", job.ID, err)
		return
	}
	log.Printf("[agent-runner] job %s: FIFO created at %s", job.ID, fifoPath)

	// Open the read end non-blocking BEFORE the subprocess opens the write end,
	// so the open() call doesn't block waiting for a writer.
	fifoReader, err := os.OpenFile(fifoPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		s.failJob(job, fmt.Sprintf("failed to open FIFO for reading: %v", err))
		log.Printf("[agent-runner] job %s: failed to open FIFO reader: %v", job.ID, err)
		return
	}

	// Open a sentinel write end so the reader's writer-count is 1, not 0.
	// With writer-count == 0, any Read() returns EOF immediately — the scanner
	// goroutine would exit before the runner process opens the real write end,
	// after which the runner's open(O_WRONLY) would block forever (no reader).
	// Keeping one write fd open prevents premature EOF; we close it only after
	// the runner subprocess exits.
	fifoSentinel, err := os.OpenFile(fifoPath, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		fifoReader.Close()
		s.failJob(job, fmt.Sprintf("failed to open FIFO sentinel: %v", err))
		log.Printf("[agent-runner] job %s: failed to open FIFO sentinel: %v", job.ID, err)
		return
	}

	runnerPath := os.Getenv("ZILLA_AGENT_RUNNER_PATH")
	if runnerPath == "" {
		fifoReader.Close()
		fifoSentinel.Close()
		s.failJob(job, "ZILLA_AGENT_RUNNER_PATH is not set")
		log.Printf("[agent-runner] job %s: ZILLA_AGENT_RUNNER_PATH is not set", job.ID)
		return
	}

	spec := agentRunnerSpec{
		JobId:         job.ID,
		FifoPath:      fifoPath,
		WorkDir:       workDir,
		SystemPrompt:  input.SystemPrompt,
		UserPrompt:    input.UserPrompt,
		Provider:      input.Provider,
		ModelId:       input.ModelId,
		MaxIterations: input.MaxIterations,
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		fifoReader.Close()
		fifoSentinel.Close()
		s.failJob(job, fmt.Sprintf("failed to marshal agent spec: %v", err))
		return
	}

	cmd := exec.Command(runnerPath, job.ID, fifoPath, workDir)
	cmd.Dir = workDir
	cmd.Stdin = bytes.NewReader(specJSON)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Assign Cmd before starting the goroutine so CancelAgentJob can safely read it.
	s.Mu.Lock()
	job.Cmd = cmd
	s.Mu.Unlock()

	if err := cmd.Start(); err != nil {
		fifoReader.Close()
		fifoSentinel.Close()
		s.failJob(job, fmt.Sprintf("failed to start agent runner: %v", err))
		log.Printf("[agent-runner] job %s: failed to start runner process: %v", job.ID, err)
		return
	}
	log.Printf("[agent-runner] job %s: runner process started (pid=%d)", job.ID, cmd.Process.Pid)

	// Start the pusher now — fifoSentinel keeps writer-count ≥ 1 so the
	// scanner blocks (via Go's runtime poller) instead of returning EOF.
	go s.startEventPusher(job.ID, fifoReader)

	// Wait for process; close sentinel so the pusher drains remaining events
	// then gets EOF, and update job status.
	go func() {
		err := cmd.Wait()
		fifoSentinel.Close()
		s.Mu.Lock()
		defer s.Mu.Unlock()
		if job.Status == JobStatusCancelled {
			log.Printf("[agent-runner] job %s: runner cancelled (pid=%d)", job.ID, cmd.Process.Pid)
		} else if err != nil {
			msg := err.Error()
			job.Error = &msg
			job.Status = JobStatusFailed
			log.Printf("[agent-runner] job %s: runner exited with error: %v", job.ID, err)
		} else {
			job.Status = JobStatusDone
			log.Printf("[agent-runner] job %s: runner exited cleanly", job.ID)
		}
		if job.CompletedAt.IsZero() {
			job.CompletedAt = time.Now()
		}
		select {
		case <-job.Done:
		default:
			close(job.Done)
		}
	}()
}

func (s *State) failJob(job *Job, msg string) {
	log.Printf("[agent-runner] job %s: FAILED — %s", job.ID, msg)
	s.Mu.Lock()
	job.Status = JobStatusFailed
	job.Error = &msg
	if job.CompletedAt.IsZero() {
		job.CompletedAt = time.Now()
	}
	select {
	case <-job.Done:
	default:
		close(job.Done)
	}
	s.Mu.Unlock()
	// Early failures (FIFO/spawn errors) happen before the event pusher exists,
	// so nothing would ever put a terminal event in the ring — the platform
	// poll loop would read "running" until eviction. Synthesize one (INC-2303).
	// No-op when the pusher is running and already saw (or will see) a real
	// terminal event: claimTerminalSynthesis is atomic and Done is closed.
	// Async because failJob can run on the HTTP request path and delivery
	// retries are slow.
	go s.synthesizeTerminalIfMissing(job.ID)
}

// CancelAgentJob sends SIGTERM to the job's process group, waits up to 5s, then
// SIGKILLs. Sets job.Status = JobStatusCancelled.
func (s *State) CancelAgentJob(jobID string) {
	s.Mu.RLock()
	job, ok := s.Jobs[jobID]
	s.Mu.RUnlock()
	if !ok {
		return
	}

	s.Mu.Lock()
	job.Status = JobStatusCancelled
	cmd := job.Cmd
	s.Mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		// No subprocess — record completion and close Done if not already done.
		s.Mu.Lock()
		if job.CompletedAt.IsZero() {
			job.CompletedAt = time.Now()
		}
		select {
		case <-job.Done:
		default:
			close(job.Done)
		}
		s.Mu.Unlock()
		return
	}

	proc := cmd.Process
	syscall.Kill(-proc.Pid, syscall.SIGTERM) //nolint:errcheck

	select {
	case <-job.Done:
		// process exited after SIGTERM
	case <-time.After(5 * time.Second):
		syscall.Kill(-proc.Pid, syscall.SIGKILL) //nolint:errcheck
	}

	select {
	case <-job.Done:
	default:
		close(job.Done)
	}
}

// GetJobEvents returns the snapshot of events for the given job, or an empty
// slice if the job is not found.
func (s *State) GetJobEvents(jobID string) [][]byte {
	s.Mu.RLock()
	job, ok := s.Jobs[jobID]
	s.Mu.RUnlock()
	if !ok || job.Events == nil {
		return [][]byte{}
	}
	return job.Events.Snapshot()
}

// PushJobEventForTest injects a raw JSON event into a job's ring buffer. It
// mirrors what the FIFO event pusher does and exists so handler tests can
// populate events deterministically (GetJob returns a snapshot without the
// ring). No-op if the job is missing or has no ring.
func (s *State) PushJobEventForTest(jobID string, event []byte) {
	s.Mu.RLock()
	job, ok := s.Jobs[jobID]
	s.Mu.RUnlock()
	if ok && job.Events != nil {
		job.Events.Push(event)
	}
}

// DevRunAgent is the MCP handler for dev_run_agent.
func DevRunAgent(ctx context.Context, req *sdk.CallToolRequest, args DevRunAgentInput) (*sdk.CallToolResult, any, error) {
	state := GetState()
	argsMap := map[string]interface{}{
		"workDir":      args.WorkDir,
		"systemPrompt": args.SystemPrompt,
		"userPrompt":   args.UserPrompt,
		"provider":     args.Provider,
		"modelId":      args.ModelId,
	}
	if args.MaxIterations != nil {
		argsMap["maxIterations"] = *args.MaxIterations
	}
	jobID := state.StartJob("dev_run_agent", argsMap)
	return &sdk.CallToolResult{
		Content: []sdk.Content{&sdk.TextContent{Text: jobID}},
	}, nil, nil
}
