// Package api provides HTTP REST response helpers and client utilities.
package api

import (
	"encoding/json"
	"net/http"
)

// ErrorResponse represents a standard JSON error payload.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

// WriteJSON serializes v into a JSON HTTP response with status code.
func WriteJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(v) //nolint:errchkjson
}

// WriteError writes a standard ErrorResponse.
func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, ErrorResponse{
		Error: msg,
		Code:  status,
	})
}
