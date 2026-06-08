package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/mathematic-inc/claude-tools-mcp/internal/tools"
)

type asyncStartRequest struct {
	Tool      string                 `json:"tool"`
	Arguments map[string]interface{} `json:"arguments"`
}

type asyncStartResponse struct {
	JobID string `json:"jobId"`
}

type jobStatusResponse struct {
	Status string      `json:"status"`
	Result interface{} `json:"result,omitempty"`
	Error  *string     `json:"error"`
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}

func handleAsyncStart(state *tools.State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req asyncStartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		if !tools.IsKnownTool(req.Tool) {
			writeJSONError(w, http.StatusBadRequest, "unknown tool")
			return
		}

		jobID := state.StartJob(req.Tool, req.Arguments)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(asyncStartResponse{JobID: jobID}); err != nil {
			log.Printf("failed to encode response: %v", err)
		}
	}
}

func handleGetJob(state *tools.State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("jobId")
		job := state.GetJob(id)
		if job == nil {
			writeJSONError(w, http.StatusNotFound, "job not found")
			return
		}

		resp := jobStatusResponse{
			Status: job.Status,
			Result: job.Result,
			Error:  job.Error,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Printf("failed to encode response: %v", err)
		}
	}
}
