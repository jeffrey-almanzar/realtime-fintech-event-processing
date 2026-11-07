package query

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "query-api"

//go:embed demo/index.html
var demoFS embed.FS

// Server exposes the read API, the live stream, and the demo page.
type Server struct {
	store       *Store
	hub         *Hub
	eventAPIURL string
	page        *template.Template
	// Heartbeat is how often an idle stream sends a comment so proxies and
	// load balancers don't close it.
	Heartbeat time.Duration
}

func NewServer(store *Store, hub *Hub, eventAPIURL string) (*Server, error) {
	page, err := template.ParseFS(demoFS, "demo/index.html")
	if err != nil {
		return nil, err
	}
	_, err = otel.Meter(instrumentationName).Int64ObservableGauge("stream.subscribers",
		metric.WithDescription("Open live-update streams"),
		metric.WithUnit("{stream}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(hub.Subscribers()))
			return nil
		}))
	if err != nil {
		return nil, err
	}
	return &Server{store: store, hub: hub, eventAPIURL: eventAPIURL, page: page, Heartbeat: 15 * time.Second}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleDemo)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /accounts/{id}", s.handleAccount)
	mux.HandleFunc("GET /accounts/{id}/transactions", s.handleTransactions)
	mux.HandleFunc("GET /accounts/{id}/events", s.handleEvents)
	return mux
}

func (s *Server) handleDemo(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.page.Execute(w, map[string]string{"EventAPIURL": s.eventAPIURL}); err != nil {
		log.Printf("Failed to render demo page: %v", err)
	}
}

func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request) {
	a, err := s.store.Account(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
	case err != nil:
		log.Printf("Failed to load account: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to load account"})
	default:
		writeJSON(w, http.StatusOK, a)
	}
}

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

func (s *Server) handleTransactions(w http.ResponseWriter, r *http.Request) {
	limit := defaultPageSize
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxPageSize {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("limit must be between 1 and %d", maxPageSize)})
			return
		}
		limit = n
	}

	txs, next, err := s.store.Transactions(r.Context(), r.PathValue("id"), r.URL.Query().Get("cursor"), limit)
	switch {
	case errors.Is(err, ErrBadCursor):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
	case err != nil:
		log.Printf("Failed to load transactions: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to load transactions"})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"transactions": txs, "next_cursor": next})
	}
}

// handleEvents streams the account's balance as Server-Sent Events: the
// current state first, then every change. Each event carries the account
// version, so a client never moves backwards to an older balance.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rc := http.NewResponseController(w)

	// Subscribe before reading the snapshot so no change can slip between
	// the two; anything older than the snapshot is filtered by version.
	sub := s.hub.Subscribe(id)
	defer sub.Close()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	w.WriteHeader(http.StatusOK)

	var sent int64
	send := func(a Account) error {
		if a.Version <= sent {
			return nil
		}
		data, err := json.Marshal(a)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: balance\nid: %d\ndata: %s\n\n", a.Version, data); err != nil {
			return err
		}
		sent = a.Version
		return rc.Flush()
	}

	snapshot, err := s.store.Account(r.Context(), id)
	switch {
	case err == nil:
		if err := send(snapshot); err != nil {
			return
		}
	case errors.Is(err, ErrNotFound):
		// No balance yet; the first applied event will arrive as an update.
		if _, err := fmt.Fprint(w, "event: not_found\ndata: {}\n\n"); err != nil || rc.Flush() != nil {
			return
		}
	default:
		log.Printf("Failed to load snapshot for %s: %v", id, err)
		return
	}

	heartbeat := time.NewTicker(s.Heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case a := <-sub.C:
			if err := send(a); err != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}
