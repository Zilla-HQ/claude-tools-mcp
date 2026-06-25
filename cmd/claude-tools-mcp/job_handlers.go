package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/mathematic-inc/claude-tools-mcp/internal/tools"
)

// handleCancelJob handles POST /jobs/{id}/cancel.
// Sends SIGTERM to job.Cmd.Process, waits 5s, then SIGKILL.
// Sets job.Status = "cancelled".
func handleCancelJob(state *tools.State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobID := r.PathValue("jobId")
		job := state.GetJob(jobID)
		if job == nil {
			writeJSONError(w, http.StatusNotFound, "job not found")
			return
		}

		state.CancelAgentJob(jobID)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		b, _ := json.Marshal(map[string]string{"status": "cancelled"})
		if _, err := w.Write(b); err != nil {
			log.Printf("failed to write cancel response: %v", err)
		}
	}
}

// handleGetJobEvents handles GET /jobs/{id}/events.
// Returns job.Events.Snapshot() as a JSON array.
func handleGetJobEvents(state *tools.State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobID := r.PathValue("jobId")
		job := state.GetJob(jobID)
		if job == nil {
			writeJSONError(w, http.StatusNotFound, "job not found")
			return
		}

		events := state.GetJobEvents(jobID)

		// Each stored event is already a JSON object encoded as []byte. Wrap as
		// json.RawMessage so the response is a JSON array of OBJECTS. Encoding
		// the raw [][]byte directly base64-encodes each element (Go marshals
		// []byte as a base64 string), which breaks consumers that read event
		// fields like `type` — the platform poll loop did exactly that and so
		// never detected terminal events (jobs appeared to run forever).
		raw := make([]json.RawMessage, len(events))
		for i, e := range events {
			raw[i] = json.RawMessage(e)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(raw); err != nil {
			log.Printf("failed to encode events: %v", err)
		}
	}
}
