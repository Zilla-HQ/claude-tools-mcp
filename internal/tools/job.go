package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	JobStatusRunning = "running"
	JobStatusDone    = "done"
	JobStatusFailed  = "failed"

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
	Done      chan struct{}
	cancel    context.CancelFunc
}

// StartJob creates a Job for the named tool, adds it to s.Jobs, and spawns a
// goroutine that executes the tool. It returns the assigned job ID.
// The job runs against context.Background() so it is not cancelled when the
// originating HTTP request context is cancelled.
func (s *State) StartJob(toolName string, args map[string]interface{}) string {
	argsJSON, _ := json.Marshal(args)

	// Use Background so the job outlives the HTTP request that started it.
	jobCtx, cancel := context.WithCancel(context.Background())

	s.Mu.Lock()
	jobID := fmt.Sprintf("job_%d", s.NextJobID)
	s.NextJobID++
	job := &Job{
		ID:        jobID,
		ToolName:  toolName,
		Arguments: argsJSON,
		Status:    JobStatusRunning,
		CreatedAt: time.Now(),
		Done:      make(chan struct{}),
		cancel:    cancel,
	}
	s.Jobs[jobID] = job
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

// evictOldJobs removes completed/failed jobs older than jobEvictAfter.
func (s *State) evictOldJobs() {
	cutoff := time.Now().Add(-jobEvictAfter)
	s.Mu.Lock()
	defer s.Mu.Unlock()
	for id, job := range s.Jobs {
		if job.Status != JobStatusRunning && job.CreatedAt.Before(cutoff) {
			delete(s.Jobs, id)
		}
	}
}

// IsKnownTool reports whether toolName is a supported async tool.
func IsKnownTool(toolName string) bool {
	switch toolName {
	case "bash", "read", "write", "edit", "glob", "grep":
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
