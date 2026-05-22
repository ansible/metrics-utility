// Mock Segment HTTP server for integration tests.
//
// Accepts any POST request, returns {"success":true} with HTTP 200, and appends
// each request body as a JSONL line (with a UTC timestamp) to an output file.
//
// Utility endpoints:
//
//	GET /requests  – return all captured requests as a JSON array
//	GET /reset     – clear the in-memory list and truncate the output file
//
// Configuration (flags override env vars):
//
//	--port   / MOCK_SEGMENT_PORT    TCP port (default: 8765)
//	--output / MOCK_SEGMENT_OUTPUT  JSONL output file path (default: /tmp/mock_segment.jsonl)
package main

import (
	"encoding/json"
	"flag"
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
	mu         sync.Mutex
	captured   []record
	outputFile string
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
	if f, ferr := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); ferr == nil {
		line, _ := json.Marshal(rec)
		f.Write(line)
		f.Write([]byte("\n"))
		f.Close()
	}
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
	os.WriteFile(outputFile, []byte{}, 0o644)
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"reset":true}`)
}

func router(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost:
		handlePost(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/requests":
		handleRequests(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/reset":
		handleReset(w, r)
	default:
		http.NotFound(w, r)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	port := flag.String("port", envOrDefault("MOCK_SEGMENT_PORT", "8765"), "TCP port to listen on")
	output := flag.String("output", envOrDefault("MOCK_SEGMENT_OUTPUT", "/tmp/mock_segment.jsonl"), "JSONL output file")
	flag.Parse()

	outputFile = *output

	addr := ":" + *port
	log.Printf("Mock Segment server listening on http://0.0.0.0%s", addr)
	log.Printf("Captured requests written to: %s", outputFile)
	log.Printf("Inspect via: GET http://localhost%s/requests", addr)
	log.Printf("Reset via:   GET http://localhost%s/reset", addr)

	http.HandleFunc("/", router)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
