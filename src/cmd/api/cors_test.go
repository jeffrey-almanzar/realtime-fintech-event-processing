package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORSPreflightFromAllowedOrigin(t *testing.T) {
	h := withCORS(http.NotFoundHandler(), "http://localhost:8082, http://example.test")
	req := httptest.NewRequest(http.MethodOptions, "/transactions", nil)
	req.Header.Set("Origin", "http://example.test")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://example.test" {
		t.Errorf("Allow-Origin = %q, want the request origin", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "Content-Type" {
		t.Errorf("Allow-Headers = %q, want Content-Type", got)
	}
}

func TestCORSAllowedOriginOnPost(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	req := httptest.NewRequest(http.MethodPost, "/transactions", nil)
	req.Header.Set("Origin", "http://localhost:8082")
	rec := httptest.NewRecorder()
	withCORS(next, "http://localhost:8082").ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want the handler's %d", rec.Code, http.StatusAccepted)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:8082" {
		t.Error("missing Allow-Origin for an allowed origin")
	}
}

func TestCORSIgnoresOtherOrigins(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/transactions", nil)
	req.Header.Set("Origin", "http://evil.test")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	withCORS(http.NotFoundHandler(), "http://localhost:8082").ServeHTTP(rec, req)

	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("Allow-Origin set for a disallowed origin")
	}
	if rec.Code == http.StatusNoContent {
		t.Error("preflight answered for a disallowed origin")
	}
}
