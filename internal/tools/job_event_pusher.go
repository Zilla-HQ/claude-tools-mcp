package tools

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// startEventPusher reads JSON-lines from the open FIFO reader and POSTs each to
// PLATFORM_WEBHOOK_URL, authenticating with the per-sandbox bearer token
// (INC-1640): X-Zilla-Token (== ZILLA_AUTH_TOKEN, the sandbox's auth_token) and
// X-Zilla-Company (== ZILLA_COMPANY_ID). This replaces the previous shared-secret
// HMAC signing — the platform owns a per-sandbox token, so a leaked credential is
// scoped to one ephemeral sandbox instead of letting any sandbox forge events for
// any company.
// Retry: 1s/2s/4s/8s backoff, max 4 attempts. On exhaustion: drop + WebhookDrops++.
// Each event is also pushed to the job's Events ring buffer.
func (s *State) startEventPusher(jobID string, fifoReader *os.File) {
	go func() {
		defer fifoReader.Close()
		log.Printf("[event-pusher] job %s: started", jobID)

		scanner := bufio.NewScanner(fifoReader)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			// Copy so the scanner buffer can be reused.
			event := make([]byte, len(line))
			copy(event, line)

			// Push to ring buffer.
			s.Mu.RLock()
			job, ok := s.Jobs[jobID]
			s.Mu.RUnlock()
			if ok && job.Events != nil {
				job.Events.Push(event)
			}

			webhookURL := os.Getenv("PLATFORM_WEBHOOK_URL")
			if webhookURL == "" {
				log.Printf("[event-pusher] PLATFORM_WEBHOOK_URL unset; dropping event for job %s", jobID)
				s.WebhookDrops.Add(1)
				continue
			}

			authToken := os.Getenv("ZILLA_AUTH_TOKEN")
			companyID := os.Getenv("ZILLA_COMPANY_ID")
			sandboxID := os.Getenv("SANDBOX_ID")

			backoffs := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
			delivered := false
			for i, wait := range backoffs {
				if postEvent(webhookURL, authToken, companyID, sandboxID, jobID, event) {
					delivered = true
					break
				}
				if i < len(backoffs)-1 {
					time.Sleep(wait)
				}
			}
			if !delivered {
				s.WebhookDrops.Add(1)
				log.Printf("[event-pusher] dropped event for job %s after max retries", jobID)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("[event-pusher] job %s: scanner error: %v", jobID, err)
		} else {
			log.Printf("[event-pusher] job %s: done (EOF)", jobID)
		}
	}()
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
