package tools

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// INC-1640: events authenticate with the per-sandbox bearer token, not the old
// shared-secret HMAC. postEvent must send X-Zilla-Token + X-Zilla-Company (and
// X-Job-Id) and must NOT send the legacy X-Sandbox-Signature header.
func TestPostEvent_SendsBearerHeaders(t *testing.T) {
	var gotToken, gotCompany, gotJob, gotSandbox, gotSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Zilla-Token")
		gotCompany = r.Header.Get("X-Zilla-Company")
		gotJob = r.Header.Get("X-Job-Id")
		gotSandbox = r.Header.Get("X-Sandbox-Id")
		gotSig = r.Header.Get("X-Sandbox-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ok := postEvent(srv.URL, "tok-abc", "company-123", "sandbox-1", "job-9", []byte(`{"type":"event","seq":1}`))

	assert.True(t, ok, "2xx response should report delivered")
	assert.Equal(t, "tok-abc", gotToken, "X-Zilla-Token must carry ZILLA_AUTH_TOKEN")
	assert.Equal(t, "company-123", gotCompany, "X-Zilla-Company must carry ZILLA_COMPANY_ID")
	assert.Equal(t, "job-9", gotJob)
	assert.Equal(t, "sandbox-1", gotSandbox)
	assert.Empty(t, gotSig, "legacy HMAC signature header must no longer be sent")
}

func TestPostEvent_Non2xxIsNotDelivered(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	ok := postEvent(srv.URL, "tok", "company", "sandbox", "job", []byte(`{"seq":1}`))
	assert.False(t, ok, "a non-2xx response must not count as delivered")
}

func TestPostEvent_UnreachableIsNotDelivered(t *testing.T) {
	// Closed server → connection refused → postEvent returns false (caller retries).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	ok := postEvent(url, "tok", "company", "sandbox", "job", []byte(`{"seq":1}`))
	assert.False(t, ok)
}

// ── INC-2303 pusher-reliability tests ────────────────────────────────────────

// newTestJob inserts a running dev_run_agent-shaped job (ring + Done channel)
// into the state and returns it.
func newTestJob(state *State, id string) *Job {
	job := &Job{
		ID:        id,
		ToolName:  "dev_run_agent",
		Status:    JobStatusRunning,
		CreatedAt: time.Now(),
		Done:      make(chan struct{}),
		cancel:    func() {},
		Events:    NewEventRing(256),
	}
	state.Mu.Lock()
	state.Jobs[id] = job
	state.Mu.Unlock()
	return job
}

func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func ringContains(job *Job, substr string) bool {
	for _, ev := range job.Events.Snapshot() {
		if bytes.Contains(ev, []byte(substr)) {
			return true
		}
	}
	return false
}

// markJobExited simulates the cmd.Wait goroutine recording the runner's exit.
func markJobExited(state *State, job *Job, status string, errMsg string) {
	state.Mu.Lock()
	job.Status = status
	if errMsg != "" {
		job.Error = &errMsg
	}
	if job.CompletedAt.IsZero() {
		job.CompletedAt = time.Now()
	}
	select {
	case <-job.Done:
	default:
		close(job.Done)
	}
	state.Mu.Unlock()
}

// INC-2303: a line larger than bufio.Scanner's default 64KB max token used to
// kill the scanner, silently dropping every subsequent event (including the
// terminal one) from both the ring and the webhook.
func TestPusherSurvivesOversizedLines(t *testing.T) {
	state := NewState()
	defer state.StopEviction()

	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	t.Setenv("PLATFORM_WEBHOOK_URL", srv.URL)

	job := newTestJob(state, "job-big-line")
	r, w, err := os.Pipe()
	require.NoError(t, err)
	state.startEventPusher(job.ID, r)

	big := `{"type":"tool.result","payload":"` + strings.Repeat("x", 200*1024) + `"}` + "\n"
	_, err = w.WriteString(big)
	require.NoError(t, err)
	_, err = w.WriteString(`{"type":"agent.done","inputTokens":1}` + "\n")
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "terminal event in ring", func() bool {
		return ringContains(job, `"agent.done"`)
	})
	waitFor(t, 5*time.Second, "both events delivered", func() bool {
		return posts.Load() >= 2
	})

	markJobExited(state, job, JobStatusDone, "")
	w.Close()

	// The real terminal event was seen — no synthetic one should follow.
	waitFor(t, 5*time.Second, "pusher to record the terminal event", func() bool {
		state.Mu.RLock()
		defer state.Mu.RUnlock()
		return job.TerminalEventSeen
	})
	time.Sleep(100 * time.Millisecond)
	assert.False(t, ringContains(job, `"synthetic"`), "no synthetic terminal expected")
	assert.Equal(t, int64(2), posts.Load())
}

// INC-2303: one wedged webhook POST (http.DefaultClient has no timeout) used to
// stall the single pusher loop, so later events — including agent.done — never
// reached the ring and the platform polled "running" for the full hour. With
// delivery decoupled behind a queue, the FIFO drain must stay live.
func TestSlowWebhookDoesNotBlockFifoDrain(t *testing.T) {
	state := NewState()
	defer state.StopEviction()

	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-unblock // wedge every request until the test ends
	}))
	defer srv.Close()
	defer close(unblock)
	t.Setenv("PLATFORM_WEBHOOK_URL", srv.URL)

	job := newTestJob(state, "job-wedged-post")
	r, w, err := os.Pipe()
	require.NoError(t, err)
	state.startEventPusher(job.ID, r)

	_, err = w.WriteString(`{"type":"tool.call","name":"bash"}` + "\n")
	require.NoError(t, err)
	_, err = w.WriteString(`{"type":"agent.done","inputTokens":1}` + "\n")
	require.NoError(t, err)

	// The first POST is wedged, but the ring (the platform's poll path) must
	// still receive the terminal event promptly.
	waitFor(t, 5*time.Second, "terminal event in ring despite wedged delivery", func() bool {
		return ringContains(job, `"agent.done"`)
	})

	markJobExited(state, job, JobStatusDone, "")
	w.Close()
}

// INC-2303 backstop: when the runner exits without a terminal event ever
// traversing the pusher (crash, EPIPE, lost line), the supervisor must
// synthesize one from the exit status so the platform can finalize.
func TestSynthesizesTerminalWhenRunnerExitsSilently(t *testing.T) {
	state := NewState()
	defer state.StopEviction()

	var lastBody atomic.Value
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		lastBody.Store(buf.String())
		posts.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	t.Setenv("PLATFORM_WEBHOOK_URL", srv.URL)

	job := newTestJob(state, "job-silent-death")
	r, w, err := os.Pipe()
	require.NoError(t, err)
	state.startEventPusher(job.ID, r)

	_, err = w.WriteString(`{"type":"tool.call","name":"bash"}` + "\n")
	require.NoError(t, err)

	// Runner dies without emitting a terminal event.
	markJobExited(state, job, JobStatusFailed, "signal: killed")
	w.Close()

	waitFor(t, 5*time.Second, "synthetic agent.error in ring", func() bool {
		return ringContains(job, `"agent.error"`) && ringContains(job, `"synthetic":true`)
	})
	waitFor(t, 5*time.Second, "synthetic event delivered", func() bool {
		last, _ := lastBody.Load().(string)
		return posts.Load() >= 2 && strings.Contains(last, "agent.error")
	})
	last, _ := lastBody.Load().(string)
	assert.Contains(t, last, "signal: killed")
}

// failJob runs before the pusher exists (FIFO/spawn failures); it must
// synthesize a terminal event so the platform poll loop doesn't read "running"
// until eviction.
func TestFailJobSynthesizesTerminal(t *testing.T) {
	state := NewState()
	defer state.StopEviction()
	t.Setenv("PLATFORM_WEBHOOK_URL", "") // delivery drops immediately; ring is the assertion

	job := newTestJob(state, "job-early-fail")
	state.failJob(job, "failed to create FIFO: boom")

	waitFor(t, 5*time.Second, "synthetic agent.error in ring after failJob", func() bool {
		return ringContains(job, `"agent.error"`) && ringContains(job, "failed to create FIFO")
	})
	snapshot := state.GetJob(job.ID)
	require.NotNil(t, snapshot)
	assert.Equal(t, JobStatusFailed, snapshot.Status)
}

// INC-2303: eviction must key off completion time, not CreatedAt — keying off
// CreatedAt evicted long-running jobs within one tick of finishing, leaving the
// platform's 30s poll a ~1-minute window before a 404.
func TestEvictOldJobsUsesCompletionTime(t *testing.T) {
	state := NewState()
	defer state.StopEviction()

	now := time.Now()
	mk := func(id, status string, createdAgo, completedAgo time.Duration) {
		job := &Job{ID: id, Status: status, CreatedAt: now.Add(-createdAgo), Done: make(chan struct{}), cancel: func() {}}
		if completedAgo > 0 {
			job.CompletedAt = now.Add(-completedAgo)
		}
		state.Mu.Lock()
		state.Jobs[id] = job
		state.Mu.Unlock()
	}

	mk("long-job-just-finished", JobStatusDone, 40*time.Minute, time.Minute)
	mk("long-job-finished-a-while-ago", JobStatusDone, 40*time.Minute, 15*time.Minute)
	mk("still-running", JobStatusRunning, 40*time.Minute, 0)
	mk("legacy-no-completed-at", JobStatusDone, 40*time.Minute, 0)

	state.evictOldJobs()

	assert.NotNil(t, state.GetJob("long-job-just-finished"), "finished 1m ago: must survive so the poll loop can observe the terminal event")
	assert.Nil(t, state.GetJob("long-job-finished-a-while-ago"), "finished 15m ago: evicted")
	assert.NotNil(t, state.GetJob("still-running"), "running jobs are never evicted")
	assert.Nil(t, state.GetJob("legacy-no-completed-at"), "zero CompletedAt falls back to CreatedAt")
}
