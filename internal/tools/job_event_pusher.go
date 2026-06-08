package tools

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// startEventPusher reads JSON-lines from the open FIFO reader, HMAC-signs each
// with SANDBOX_WEBHOOK_SECRET, and POSTs to PLATFORM_WEBHOOK_URL.
// Retry: 1s/2s/4s/8s backoff, max 4 attempts. On exhaustion: drop + WebhookDrops++.
// Each event is also pushed to the job's Events ring buffer.
func (s *State) startEventPusher(jobID string, fifoReader *os.File) {
	go func() {
		defer fifoReader.Close()

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

			secret := os.Getenv("SANDBOX_WEBHOOK_SECRET")
			sandboxID := os.Getenv("SANDBOX_ID")

			sig := computeHMAC(secret, event)

			backoffs := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
			delivered := false
			for i, wait := range backoffs {
				if postEvent(webhookURL, sig, sandboxID, event) {
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
	}()
}

func computeHMAC(secret string, data []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(data)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func postEvent(url, sig, sandboxID string, body []byte) bool {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Sandbox-Signature", sig)
	req.Header.Set("X-Sandbox-Id", sandboxID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
