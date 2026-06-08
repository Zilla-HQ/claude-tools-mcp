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

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(events); err != nil {
			log.Printf("failed to encode events: %v", err)
		}
	}
}
