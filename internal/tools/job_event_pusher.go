package tools

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	// maxEventLineBytes bounds a single FIFO event line. bufio.Scanner's default
	// 64KB max token silently killed the pusher on oversized span payloads
	// (INC-2303): scanner.Err() fired, the FIFO stopped draining, the runner
	// blocked on its next write, and the job hung as "running" until the
	// platform's 1h poll ceiling.
	maxEventLineBytes = 8 * 1024 * 1024

	// deliveryQueueBuffer is how many events may be pending webhook delivery
	// before new ones are dropped (ring-buffer push is unaffected — the poll
	// path still sees them). Sized generously: an agent run emits hundreds of
	// events, not hundreds of thousands.
	deliveryQueueBuffer = 1024

	// postEventTimeout bounds one webhook POST attempt. http.DefaultClient has
	// no timeout, so a half-open connection (platform redeploy, LB drop) wedged
	// the old single-loop pusher forever (INC-2303).
	postEventTimeout = 30 * time.Second
)

// webhookClient is the shared HTTP client for event delivery. Never use
// http.DefaultClient here — see postEventTimeout.
var webhookClient = &http.Client{Timeout: postEventTimeout}

// terminalEventTypes mirrors the platform's TERMINAL_TYPES
// (handlers/sandbox-event.ts, sandbox/client.ts getJobTerminalStatus).
var terminalEventTypes = map[string]struct{}{
	"agent.done":         {},
	"agent.error":        {},
	"agent.cancelled":    {},
	"agent.out_of_turns": {},
}

func isTerminalEvent(event []byte) bool {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(event, &probe); err != nil {
		return false
	}
	_, ok := terminalEventTypes[probe.Type]
	return ok
}

// startEventPusher reads JSON-lines from the open FIFO reader, pushes each into
// the job's Events ring (the platform poll path), and queues it for webhook
// delivery to PLATFORM_WEBHOOK_URL, authenticating with the per-sandbox bearer
// token (INC-1640): X-Zilla-Token (== ZILLA_AUTH_TOKEN) and X-Zilla-Company
// (== ZILLA_COMPANY_ID).
//
// INC-2303 hardening — the FIFO drain and webhook delivery are decoupled:
// delivery runs on its own goroutine behind a buffered queue, so a slow or
// wedged POST can never stop the FIFO from draining (a blocked FIFO writer is
// what turned one bad POST into a silently-hung job). Delivery retry:
// 1s/2s/4s/8s backoff, max 4 attempts; on exhaustion or a full queue the event
// is dropped from the webhook path only (WebhookDrops++) — the ring still has
// it. After the stream ends, synthesizeTerminalIfMissing backstops jobs whose
// terminal event never made it out of the runner.
func (s *State) startEventPusher(jobID string, fifoReader *os.File) {
	go func() {
		log.Printf("[event-pusher] job %s: started", jobID)

		queue := make(chan []byte, deliveryQueueBuffer)
		deliveryDone := make(chan struct{})
		go func() {
			defer close(deliveryDone)
			for event := range queue {
				s.deliverEvent(jobID, event)
			}
		}()

		scanner := bufio.NewScanner(fifoReader)
		scanner.Buffer(make([]byte, 64*1024), maxEventLineBytes)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			// Copy so the scanner buffer can be reused.
			event := make([]byte, len(line))
			copy(event, line)

			// Ring first: the poll path must see everything delivery sees, and
			// must see it even if delivery later fails.
			s.Mu.RLock()
			job, ok := s.Jobs[jobID]
			s.Mu.RUnlock()
			if ok && job.Events != nil {
				job.Events.Push(event)
			}
			if isTerminalEvent(event) {
				s.markTerminalEventSeen(jobID)
			}

			select {
			case queue <- event:
			default:
				s.WebhookDrops.Add(1)
				log.Printf("[event-pusher] job %s: delivery queue full; dropping event from webhook path", jobID)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("[event-pusher] job %s: scanner error: %v", jobID, err)
		} else {
			log.Printf("[event-pusher] job %s: done (EOF)", jobID)
		}

		// Close the reader BEFORE waiting on anything: after a scanner error
		// the runner may still be writing, and an open-but-unread FIFO blocks
		// it forever once the pipe buffer fills. Closing gives the writer
		// EPIPE so it can exit and cmd.Wait can record a status.
		fifoReader.Close()

		// Synthesize BEFORE draining the delivery queue: with a wedged webhook
		// each queued event can burn ~2 min of POST timeouts, and the whole
		// point of synthesis is to put a terminal event where the platform
		// poll loop can see it (the ring) promptly. The synthetic's own
		// webhook copy rides the queue like any other event.
		s.synthesizeTerminalIfMissing(jobID, queue)

		close(queue)
		<-deliveryDone
	}()
}

// markTerminalEventSeen records that a terminal event for the job traversed the
// pusher (into the ring), so the synthesis backstop knows not to fire.
func (s *State) markTerminalEventSeen(jobID string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if job, ok := s.Jobs[jobID]; ok {
		job.TerminalEventSeen = true
	}
}

// claimTerminalSynthesis atomically checks-and-sets TerminalEventSeen so that
// concurrent callers (pusher end-of-stream, failJob) synthesize at most once.
// Returns the job and true when the caller won the claim.
func (s *State) claimTerminalSynthesis(jobID string) (*Job, bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	job, ok := s.Jobs[jobID]
	if !ok || job.TerminalEventSeen {
		return nil, false
	}
	job.TerminalEventSeen = true
	return job, true
}

// synthesizeTerminalIfMissing is the INC-2303 backstop: the supervisor knows
// from cmd.Wait when the runner process is gone, but historically only flipped
// an internal status the platform never reads — the terminal signal existed
// solely as a FIFO message. If the runner exits (crash, EPIPE, lost line)
// without a terminal event having traversed the pusher, synthesize one from the
// job's exit status and emit it on BOTH paths (ring for the poll loop, webhook
// for the push path) so the platform can finalize instead of hanging to its 1h
// ceiling.
//
// Blocks until the job's Done channel closes (exit status recorded); intended
// to run on the pusher goroutine after the stream ends. The ring push happens
// immediately; the webhook copy is enqueued on `queue` when non-nil (the
// pusher's delivery queue — never blocks) or POSTed directly when nil (the
// failJob path, where no pusher exists).
func (s *State) synthesizeTerminalIfMissing(jobID string, queue chan<- []byte) {
	s.Mu.RLock()
	job, ok := s.Jobs[jobID]
	s.Mu.RUnlock()
	if !ok {
		return
	}

	<-job.Done

	job, ok = s.claimTerminalSynthesis(jobID)
	if !ok {
		return
	}

	s.Mu.RLock()
	status := job.Status
	var errMsg string
	if job.Error != nil {
		errMsg = *job.Error
	}
	s.Mu.RUnlock()

	var evType string
	switch status {
	case JobStatusCancelled:
		evType = "agent.cancelled"
	case JobStatusDone:
		// A clean exit (0) is ambiguous: the runner exits 0 for agent.done AND
		// agent.out_of_turns, and the whole reason we're synthesizing is that
		// the real terminal line was lost. Synthesizing agent.done here could
		// declare success on incomplete (out-of-turns) work and let the
		// platform commit/finalize it. Fail conservatively instead: a false
		// failure costs a retry; a false success ships incomplete work.
		evType = "agent.error"
		errMsg = "runner exited cleanly but its terminal event was lost; failing conservatively (a lost agent.out_of_turns is indistinguishable from a lost agent.done)"
	default:
		evType = "agent.error"
		if errMsg == "" {
			errMsg = "runner exited without emitting a terminal event"
		}
	}

	synthetic := map[string]any{
		"type":      evType,
		"synthetic": true,
	}
	if evType == "agent.error" {
		synthetic["error"] = errMsg
	}
	payload, err := json.Marshal(synthetic)
	if err != nil {
		return
	}

	log.Printf("[event-pusher] job %s: runner exited (status=%s) without a terminal event; synthesizing %s", jobID, status, evType)
	if job.Events != nil {
		job.Events.Push(payload)
	}
	if queue != nil {
		select {
		case queue <- payload:
		default:
			s.WebhookDrops.Add(1)
			log.Printf("[event-pusher] job %s: delivery queue full; dropping synthetic terminal from webhook path", jobID)
		}
	} else {
		s.deliverEvent(jobID, payload)
	}
}

// deliverEvent POSTs one event to the platform webhook with bounded retries.
// On exhaustion the event is dropped from the webhook path (WebhookDrops++);
// the ring buffer still holds it for the platform's poll loop.
func (s *State) deliverEvent(jobID string, event []byte) {
	webhookURL := os.Getenv("PLATFORM_WEBHOOK_URL")
	if webhookURL == "" {
		log.Printf("[event-pusher] PLATFORM_WEBHOOK_URL unset; dropping event for job %s", jobID)
		s.WebhookDrops.Add(1)
		return
	}

	authToken := os.Getenv("ZILLA_AUTH_TOKEN")
	companyID := os.Getenv("ZILLA_COMPANY_ID")
	sandboxID := os.Getenv("SANDBOX_ID")

	backoffs := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	for i, wait := range backoffs {
		if postEvent(webhookURL, authToken, companyID, sandboxID, jobID, event) {
			return
		}
		if i < len(backoffs)-1 {
			time.Sleep(wait)
		}
	}
	s.WebhookDrops.Add(1)
	log.Printf("[event-pusher] dropped event for job %s after max retries", jobID)
}

func postEvent(url, authToken, companyID, sandboxID, jobID string, body []byte) bool {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	// INC-1640: per-sandbox bearer auth. The platform validates X-Zilla-Token
	// against the sandbox row's auth_token (timing-safe) and X-Zilla-Company
	// against the job's owning company. Replaces the shared-secret HMAC
	// (X-Sandbox-Signature), which is being retired.
	req.Header.Set("X-Zilla-Token", authToken)
	req.Header.Set("X-Zilla-Company", companyID)
	req.Header.Set("X-Sandbox-Id", sandboxID)
	req.Header.Set("X-Job-Id", jobID)

	resp, err := webhookClient.Do(req)
	if err != nil {
		return false
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
