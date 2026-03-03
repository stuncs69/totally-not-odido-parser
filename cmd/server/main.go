package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"gurty/internal/data"
)

type apiServer struct {
	store *data.Store
}

func main() {
	var (
		addr        = flag.String("addr", ":8080", "HTTP listen address")
		datasetPath = flag.String("dataset", "dataset.txt", "path to dataset.txt JSONL file")
		dbPath      = flag.String("db", "dataset.sqlite", "path to SQLite index file")
	)
	flag.Parse()

	if _, err := os.Stat(*datasetPath); err != nil {
		log.Fatalf("dataset missing: %v", err)
	}

	store, err := data.NewStore(*datasetPath, *dbPath)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("ensuring index at %s ...", *dbPath)
	if err := store.EnsureIndex(ctx); err != nil {
		log.Fatalf("failed to ensure index: %v", err)
	}
	log.Printf("index ready")

	srv := &apiServer{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", srv.handleHealth)
	mux.HandleFunc("/api/stats", srv.handleStats)
	mux.HandleFunc("/api/facets", srv.handleFacets)
	mux.HandleFunc("/api/records", srv.handleRecords)
	mux.HandleFunc("/api/records/", srv.handleRecordDetail)
	mux.HandleFunc("/api/analytics/fields", srv.handleAnalyticsFields)
	mux.HandleFunc("/api/analytics/distribution", srv.handleAnalyticsDistribution)
	mux.HandleFunc("/api/analytics/count", srv.handleAnalyticsCount)
	mux.HandleFunc("/analytics", srv.handleAnalyticsPage)
	mux.Handle("/", http.FileServer(http.Dir("static")))

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("listening on http://localhost%s", *addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server crashed: %v", err)
	}
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	health, err := s.store.Health(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, health)
}

func (s *apiServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	stats, err := s.store.Stats(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *apiServer) handleFacets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if parsed > 500 {
			parsed = 500
		}
		limit = parsed
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	facets, err := s.store.Facets(ctx, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, facets)
}

func (s *apiServer) handleRecords(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	q := r.URL.Query()

	limit := 50
	if v := q.Get("limit"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = p
	}

	offset := 0
	if v := q.Get("offset"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid offset")
			return
		}
		offset = p
	}

	params := data.QueryParams{
		Q:            strings.TrimSpace(q.Get("q")),
		Type:         strings.TrimSpace(q.Get("type")),
		Status:       strings.TrimSpace(q.Get("status")),
		Country:      strings.TrimSpace(q.Get("country")),
		City:         strings.TrimSpace(q.Get("city")),
		Active:       strings.TrimSpace(q.Get("active")),
		ModifiedFrom: strings.TrimSpace(q.Get("modified_from")),
		ModifiedTo:   strings.TrimSpace(q.Get("modified_to")),
		Sort:         strings.TrimSpace(q.Get("sort")),
		Limit:        limit,
		Offset:       offset,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	result, err := s.store.QueryRecords(ctx, params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *apiServer) handleRecordDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	raw := strings.TrimPrefix(r.URL.Path, "/api/records/")
	if raw == "" {
		writeError(w, http.StatusNotFound, "missing row number")
		return
	}
	rowNum, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || rowNum <= 0 {
		writeError(w, http.StatusBadRequest, "invalid row number")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	payload, err := s.store.RecordJSON(ctx, rowNum)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Write as a single JSON document with the raw record as nested object.
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"row_num":` + strconv.FormatInt(rowNum, 10) + `,"data":`))
	_, _ = w.Write(payload)
	_, _ = w.Write([]byte(`}`))
}

func (s *apiServer) handleAnalyticsPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if r.URL.Path != "/analytics" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	http.ServeFile(w, r, "static/analytics.html")
}

func (s *apiServer) handleAnalyticsFields(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	fields, err := s.store.AnalyticsFields(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"fields": fields,
	})
}

func (s *apiServer) handleAnalyticsDistribution(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	field := strings.TrimSpace(r.URL.Query().Get("field"))
	if field == "" {
		writeError(w, http.StatusBadRequest, "missing field")
		return
	}

	limit := 25
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}

	filter := strings.TrimSpace(r.URL.Query().Get("filter"))

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	result, err := s.store.AnalyticsDistribution(ctx, field, filter, limit)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *apiServer) handleAnalyticsCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	field := strings.TrimSpace(r.URL.Query().Get("field"))
	if field == "" {
		writeError(w, http.StatusBadRequest, "missing field")
		return
	}

	if _, ok := r.URL.Query()["value"]; !ok {
		writeError(w, http.StatusBadRequest, "missing value")
		return
	}
	value := r.URL.Query().Get("value")

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	result, err := s.store.AnalyticsCount(ctx, field, value)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s (%s)", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}
