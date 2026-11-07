package main

import (
	"net/http"
	"strings"
)

// withCORS lets browser pages on the allowed origins (such as the demo page
// served by the Query API) call this API. Other origins get no CORS headers,
// so browsers block their requests.
func withCORS(next http.Handler, allowedOrigins string) http.Handler {
	allowed := map[string]bool{}
	for _, o := range strings.Split(allowedOrigins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			allowed[o] = true
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			// Preflight: the browser asks before sending a JSON POST.
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				w.Header().Set("Access-Control-Allow-Methods", "POST")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				w.Header().Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
