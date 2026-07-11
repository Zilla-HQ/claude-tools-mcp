package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"time"

	"github.com/google/uuid"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// JobSnapshot is a locked copy of a Job's fields, safe to read without holding
// State.Mu. GetJob returns a snapshot so callers never observe a partially
// updated Job under a race.
type JobSnapshot struct {
	ID        string
	ToolName  string
	Arguments json.RawMessage
	Status    string
	Result    *sdk.CallToolResult
	Error     *string
	CreatedAt time.Time
}

const (
	JobStatusRunning   = "running"
	JobStatusDone      = "done"
	JobStatusFailed    = "failed"
	JobStatusCancelled = "cancelled"

	jobEvictAfter = 10 * time.Minute
	jobEvictTick  = time.Minute
)

// Job represents an async tool call that executes in the background.
type Job struct {
	ID        string
	ToolName  string
	Arguments json.RawMessage
	Status    string
	Result    *sdk.CallToolResult
	Error     *string
	CreatedAt time.Time
	// CompletedAt is set when the job reaches a non-running status. Eviction
	// keys off it (INC-2303): keying off CreatedAt evicted long-running jobs
	// within one tick of finishing, leaving the platform's 30s poll loop a
	// ~1-minute window to observe the terminal event before a 404.
	CompletedAt time.Time
	Done        chan struct{}
	cancel      context.CancelFunc
	Cmd         *exec.Cmd
	Events      *EventRing
	// TerminalEventSeen records that a terminal agent event traversed the
	// event pusher (or was synthesized). Guarded by State.Mu. See
	// synthesizeTerminalIfMissing (INC-2303).
	TerminalEventSeen bool
}

// StartJob creates a Job for the named tool, adds it to s.Jobs, and spawns a
// goroutine that executes the tool. It returns the assigned job ID.
// The job runs against context.Background() so it is not cancelled when the
// originating HTTP request context is cancelled.
//
// dev_run_agent is handled specially: it initialises the EventRing and
// delegates to runAgentJob (FIFO + subprocess). All other tools go through
// dispatchTool and store their result in Job.Result.
func (s *State) StartJob(toolName string, args map[string]interface{}) string {
	argsJSON, _ := json.Marshal(args)

	s.Mu.Lock()
	jobID := uuid.New().String()
	job := &Job{
		ID:        jobID,
		ToolName:  toolName,
		Arguments: argsJSON,
		Status:    JobStatusRunning,
		CreatedAt: time.Now(),
		Done:      make(chan struct{}),
		cancel:    func() {},
	}
	s.Jobs[jobID] = job
	s.Mu.Unlock()

	if toolName == "dev_run_agent" {
		var input DevRunAgentInput
		if err := json.Unmarshal(argsJSON, &input); err != nil {
			s.failJob(job, fmt.Sprintf("invalid dev_run_agent arguments: %v", err))
			return jobID
		}
		job.Events = NewEventRing(256)
		log.Printf("[agent-runner] job %s: starting (model=%s provider=%s workDir=%s)", jobID, input.ModelId, input.Provider, input.WorkDir)
		go s.runAgentJob(job, input)
		return jobID
	}

	// Generic tool path: run to completion, store result in Job.Result.
	jobCtx, cancel := context.WithCancel(context.Background())
	s.Mu.Lock()
	job.cancel = cancel
	s.Mu.Unlock()

	go func() {
		defer cancel()
		result, err := s.dispatchTool(jobCtx, toolName, argsJSON)
		s.Mu.Lock()
		defer s.Mu.Unlock()
		if err != nil {
			msg := err.Error()
			job.Error = &msg
			job.Status = JobStatusFailed
		} else {
			job.Result = result
			job.Status = JobStatusDone
		}
		job.CompletedAt = time.Now()
		close(job.Done)
	}()

	return jobID
}

// GetJob returns a snapshot of the job with the given ID, or nil if not found.
// The snapshot is copied under the read lock so callers read consistent fields
// without holding the lock.
func (s *State) GetJob(id string) *JobSnapshot {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	job, ok := s.Jobs[id]
	if !ok {
		return nil
	}
	return &JobSnapshot{
		ID:        job.ID,
		ToolName:  job.ToolName,
		Arguments: job.Arguments,
		Status:    job.Status,
		Result:    job.Result,
		Error:     job.Error,
		CreatedAt: job.CreatedAt,
	}
}

// evictOldJobs removes completed/failed jobs that finished more than
// jobEvictAfter ago. Falls back to CreatedAt when CompletedAt was never set
// (defensive; every status-flip site sets it).
func (s *State) evictOldJobs() {
	cutoff := time.Now().Add(-jobEvictAfter)
	s.Mu.Lock()
	defer s.Mu.Unlock()
	for id, job := range s.Jobs {
		if job.Status == JobStatusRunning {
			continue
		}
		ref := job.CompletedAt
		if ref.IsZero() {
			ref = job.CreatedAt
		}
		if ref.Before(cutoff) {
			delete(s.Jobs, id)
		}
	}
}

// IsKnownTool reports whether toolName is a supported async tool.
func IsKnownTool(toolName string) bool {
	switch toolName {
	case "bash", "read", "write", "edit", "glob", "grep", "dev_run_agent":
		return true
	}
	return false
}

// dispatchTool routes a tool call by name and returns a *sdk.CallToolResult.
func (s *State) dispatchTool(ctx context.Context, toolName string, argsJSON json.RawMessage) (*sdk.CallToolResult, error) {
	switch toolName {
	case "bash":
		var input BashInput
		if err := json.Unmarshal(argsJSON, &input); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		result, _, err := Bash(ctx, nil, input)
		return result, err
	case "read":
		var input ReadInput
		if err := json.Unmarshal(argsJSON, &input); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		result, _, err := Read(ctx, nil, input)
		return result, err
	case "write":
		var input WriteInput
		if err := json.Unmarshal(argsJSON, &input); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		result, _, err := Write(ctx, nil, input)
		return result, err
	case "edit":
		var input EditInput
		if err := json.Unmarshal(argsJSON, &input); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		result, _, err := Edit(ctx, nil, input)
		return result, err
	case "glob":
		var input GlobInput
		if err := json.Unmarshal(argsJSON, &input); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		result, _, err := Glob(ctx, nil, input)
		return result, err
	case "grep":
		var input GrepInput
		if err := json.Unmarshal(argsJSON, &input); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		result, _, err := Grep(ctx, nil, input)
		return result, err
	default:
		return nil, fmt.Errorf("unknown tool: %s", toolName)
	}
}
