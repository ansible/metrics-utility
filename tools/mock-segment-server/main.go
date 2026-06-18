// Mock Segment HTTP server for integration tests.
//
// Accepts any POST request, returns {"success":true} with HTTP 200, and
// captures each request body in memory.
//
// Utility endpoints:
//
//	GET  /requests – return all captured requests as a JSON array
//	POST /reset    – clear the captured request list
//
// Configuration (env vars):
//
//	MOCK_SEGMENT_PORT  TCP port (default: 8765)
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type record struct {
	Timestamp string          `json:"timestamp"`
	Path      string          `json:"path"`
	Body      json.RawMessage `json:"body"`
}

var (
	mu       sync.Mutex
	captured []record
)

func handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return
	}

	rec := record{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Path:      r.URL.Path,
		Body:      json.RawMessage(body),
	}

	mu.Lock()
	captured = append(captured, rec)
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"success":true}`)
}

func handleRequests(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	data, _ := json.Marshal(captured)
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func handleReset(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	captured = nil
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"reset":true}`)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	addr := ":" + envOrDefault("MOCK_SEGMENT_PORT", "8765")
	log.Printf("Mock Segment server listening on http://0.0.0.0%s", addr)
	log.Printf("Inspect via: GET http://localhost%s/requests", addr)
	log.Printf("Reset via:   POST http://localhost%s/reset", addr)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /reset", handleReset)
	mux.HandleFunc("GET /requests", handleRequests)
	mux.HandleFunc("POST /", handlePost)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
