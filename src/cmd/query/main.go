// Query API: serves account balances and history from Postgres and streams
// balance changes to clients with Server-Sent Events. Also serves a demo page
// at http://localhost:8082.

package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/query"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Init(ctx, "query-api")
	if err != nil {
		log.Fatalf("Failed to initialize telemetry: %v", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			log.Printf("Telemetry shutdown error: %v", err)
		}
	}()

	store, err := query.NewStore(ctx, config.DatabaseURL())
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer store.Close()

	hub := query.NewHub()
	listener, err := query.NewListener(config.DatabaseURL(), hub, store)
	if err != nil {
		log.Fatalf("Failed to create listener: %v", err)
	}
	go listener.Run(ctx)

	server, err := query.NewServer(store, hub, config.EventAPIURL())
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	srv := &http.Server{
		Addr: config.QueryHTTPAddr(),
		Handler: otelhttp.NewHandler(server.Handler(), "http.server",
			otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.Pattern }),
			// A stream is one request lasting minutes; tracing it adds nothing.
			otelhttp.WithFilter(func(r *http.Request) bool { return !strings.HasSuffix(r.URL.Path, "/events") })),
		ReadHeaderTimeout: 5 * time.Second,
		// No WriteTimeout: live streams stay open indefinitely.
	}

	go func() {
		<-ctx.Done()
		log.Print("Shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Streams never finish on their own, so close them rather than wait.
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
		}
	}()

	log.Printf("Query API listening on %s (demo page at http://localhost%s)", srv.Addr, srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server error: %v", err)
	}
}
