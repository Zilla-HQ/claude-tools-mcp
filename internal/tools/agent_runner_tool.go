package tools

import (
	"context"
	"fmt"
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
	WorkDir string `json:"workDir"`
}

// StartAgentJob allocates a job ID, creates the per-job FIFO under
// $ZILLA_JOB_RUNTIME_DIR/<jobId>/events.fifo, spawns $ZILLA_AGENT_RUNNER_PATH,
// and starts the event pusher goroutine. Returns the job ID.
func (s *State) StartAgentJob(workDir string) string {
	s.Mu.Lock()
	jobID := fmt.Sprintf("job_%d", s.NextJobID)
	s.NextJobID++
	job := &Job{
		ID:       jobID,
		ToolName: "dev_run_agent",
		Status:   JobStatusRunning,
		Done:     make(chan struct{}),
		cancel:   func() {},
		Events:   NewEventRing(256),
	}
	s.Jobs[jobID] = job
	s.Mu.Unlock()

	go s.runAgentJob(job, workDir)

	return jobID
}

func (s *State) runAgentJob(job *Job, workDir string) {
	defer job.cancel()

	runtimeDir := os.Getenv("ZILLA_JOB_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = os.TempDir()
	}

	jobDir := filepath.Join(runtimeDir, job.ID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		s.failJob(job, fmt.Sprintf("failed to create job dir: %v", err))
		return
	}

	fifoPath := filepath.Join(jobDir, "events.fifo")
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		s.failJob(job, fmt.Sprintf("failed to create FIFO: %v", err))
		return
	}

	// Open the read end non-blocking BEFORE the subprocess opens the write end,
	// so the open() call doesn't block waiting for a writer.
	fifoReader, err := os.OpenFile(fifoPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		s.failJob(job, fmt.Sprintf("failed to open FIFO for reading: %v", err))
		return
	}

	runnerPath := os.Getenv("ZILLA_AGENT_RUNNER_PATH")
	if runnerPath == "" {
		fifoReader.Close()
		s.failJob(job, "ZILLA_AGENT_RUNNER_PATH is not set")
		return
	}

	cmd := exec.Command(runnerPath, job.ID, fifoPath, workDir)
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Assign Cmd before starting the goroutine so CancelAgentJob can safely read it.
	s.Mu.Lock()
	job.Cmd = cmd
	s.Mu.Unlock()

	if err := cmd.Start(); err != nil {
		fifoReader.Close()
		s.failJob(job, fmt.Sprintf("failed to start agent runner: %v", err))
		return
	}

	// Only start the pusher after the subprocess is running — it will open the write end.
	go s.startEventPusher(job.ID, fifoReader)

	// Wait for process; update job status when done.
	go func() {
		err := cmd.Wait()
		s.Mu.Lock()
		defer s.Mu.Unlock()
		if job.Status == JobStatusCancelled {
			// already transitioned by cancel handler
		} else if err != nil {
			msg := err.Error()
			job.Error = &msg
			job.Status = JobStatusFailed
		} else {
			job.Status = JobStatusDone
		}
		select {
		case <-job.Done:
		default:
			close(job.Done)
		}
	}()
}

func (s *State) failJob(job *Job, msg string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	job.Status = JobStatusFailed
	job.Error = &msg
	select {
	case <-job.Done:
	default:
		close(job.Done)
	}
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
	s.Mu.Unlock()

	s.Mu.RLock()
	cmd := job.Cmd
	s.Mu.RUnlock()

	if cmd == nil || cmd.Process == nil {
		// No subprocess — just close Done channel if not already done.
		select {
		case <-job.Done:
		default:
			close(job.Done)
		}
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

// DevRunAgent is the MCP handler for dev_run_agent.
func DevRunAgent(ctx context.Context, req *sdk.CallToolRequest, args DevRunAgentInput) (*sdk.CallToolResult, any, error) {
	state := GetState()
	jobID := state.StartAgentJob(args.WorkDir)
	return &sdk.CallToolResult{
		Content: []sdk.Content{&sdk.TextContent{Text: jobID}},
	}, nil, nil
}
