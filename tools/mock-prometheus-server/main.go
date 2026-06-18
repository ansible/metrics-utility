// Mock Prometheus HTTP server for integration tests.
//
// Implements just enough of the Prometheus HTTP API for the vCPU collector's
// PrometheusClient to work:
//
//	GET /api/v1/query        – instant query, returns a single vector result
//	GET /api/v1/query_range  – range query, generates time-series at step intervals
//
// Utility endpoints:
//
//	GET  /requests  – return all captured requests as a JSON array
//	POST /reset     – clear the captured request list and restore default config
//	GET  /config    – return current configuration
//	POST /config    – update configuration (e.g. {"cpu_value": "24", "empty_result": true})
//
// Configuration (env vars):
//
//	MOCK_PROMETHEUS_PORT  TCP port (default: 9090)
//
// CPU values default to ["16"] and can be changed at runtime via POST /config.
// POST /config accepts both "cpu_value": "16" and "cpu_value": ["16","32"].
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type record struct {
	Timestamp string            `json:"timestamp"`
	Path      string            `json:"path"`
	Params    map[string]string `json:"params"`
}

type stringOrSlice []string

func (s *stringOrSlice) UnmarshalJSON(data []byte) error {
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		*s = arr
		return nil
	}

	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = []string{single}
		return nil
	}

	return fmt.Errorf("cpu_value must be a string or array of strings")
}

type config struct {
	CPUValues   stringOrSlice `json:"cpu_value"`
	EmptyResult bool          `json:"empty_result"`
}

var defaultConfig = config{CPUValues: []string{"16"}}

var (
	mu       sync.Mutex
	captured = []record{}
	cfg      config
)

func capture(r *http.Request) {
	params := make(map[string]string)
	for k, v := range r.URL.Query() {
		params[k] = v[0]
	}

	rec := record{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Path:      r.URL.Path,
		Params:    params,
	}

	mu.Lock()
	captured = append(captured, rec)
	mu.Unlock()
}

func getConfig() config {
	mu.Lock()
	c := cfg
	mu.Unlock()
	return c
}

// parseDuration handles simple Prometheus duration strings: a number followed
// by one of s, m, h (e.g. "5m", "300s", "1h"). A bare number is treated as
// seconds.
func parseDuration(s string) (float64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	multiplier := 1.0
	numStr := s

	last := s[len(s)-1]
	switch last {
	case 's':
		numStr = s[:len(s)-1]
	case 'm':
		numStr = s[:len(s)-1]
		multiplier = 60
	case 'h':
		numStr = s[:len(s)-1]
		multiplier = 3600
	default:
		if last < '0' || last > '9' {
			return 0, fmt.Errorf("unsupported duration suffix: %c", last)
		}
	}

	n, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration number: %s", numStr)
	}

	return n * multiplier, nil
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	capture(r)

	c := getConfig()

	w.Header().Set("Content-Type", "application/json")

	if c.EmptyResult {
		json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result":     []any{},
			},
		})
		return
	}

	ts := float64(time.Now().Unix())
	if timeStr := r.URL.Query().Get("time"); timeStr != "" {
		if parsed, err := strconv.ParseFloat(timeStr, 64); err == nil {
			ts = parsed
		}
	}

	cpuVal := c.CPUValues[len(c.CPUValues)-1]

	json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result": []any{
				map[string]any{
					"metric": map[string]any{},
					"value":  []any{ts, cpuVal},
				},
			},
		},
	})
}

func handleQueryRange(w http.ResponseWriter, r *http.Request) {
	capture(r)

	c := getConfig()

	w.Header().Set("Content-Type", "application/json")

	if c.EmptyResult {
		json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "matrix",
				"result":     []any{},
			},
		})
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	stepStr := r.URL.Query().Get("step")

	start, _ := strconv.ParseFloat(startStr, 64)
	end, _ := strconv.ParseFloat(endStr, 64)

	stepSecs := 300.0 // default 5m
	if stepStr != "" {
		if parsed, err := parseDuration(stepStr); err == nil {
			stepSecs = parsed
		}
	}

	count := int(math.Floor((end-start)/stepSecs)) + 1
	values := make([][]any, 0, count)
	for i := 0; i < count; i++ {
		ts := start + float64(i)*stepSecs
		if ts > end {
			break
		}
		cpuVal := c.CPUValues[i%len(c.CPUValues)]
		values = append(values, []any{ts, cpuVal})
	}

	json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "matrix",
			"result": []any{
				map[string]any{
					"metric": map[string]any{},
					"values": values,
				},
			},
		},
	})
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
	captured = []record{}
	cfg = defaultConfig
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"reset":true}`)
}

func handleConfigGet(w http.ResponseWriter, _ *http.Request) {
	c := getConfig()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c)
}

func handleConfigPost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var update config
	if err := json.Unmarshal(body, &update); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	mu.Lock()
	if len(update.CPUValues) > 0 {
		cfg.CPUValues = update.CPUValues
	}
	cfg.EmptyResult = update.EmptyResult
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"ok":true}`)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	cfg = defaultConfig

	addr := ":" + envOrDefault("MOCK_PROMETHEUS_PORT", "9090")
	log.Printf("Mock Prometheus server listening on http://0.0.0.0%s", addr)
	log.Printf("Inspect via: GET http://localhost%s/requests", addr)
	log.Printf("Reset via:   POST http://localhost%s/reset", addr)
	log.Printf("Config via:  GET/POST http://localhost%s/config", addr)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/query", handleQuery)
	mux.HandleFunc("GET /api/v1/query_range", handleQueryRange)
	mux.HandleFunc("GET /requests", handleRequests)
	mux.HandleFunc("POST /reset", handleReset)
	mux.HandleFunc("GET /config", handleConfigGet)
	mux.HandleFunc("POST /config", handleConfigPost)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
