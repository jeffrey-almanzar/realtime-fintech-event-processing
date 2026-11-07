package telemetry

import (
	"log"
	"net/http"
	_ "net/http/pprof" // registers the /debug/pprof handlers on DefaultServeMux
	"os"
)

// StartPprof serves Go profiles on PPROF_ADDR when it is set, for example
// localhost:6060. Keep it bound to localhost: profiles expose internals.
func StartPprof() {
	addr := os.Getenv("PPROF_ADDR")
	if addr == "" {
		return
	}
	go func() {
		log.Printf("pprof listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("pprof server stopped: %v", err)
		}
	}()
}
