#!/usr/bin/env bash
# Exposes the cluster's services on localhost until stopped, or, when given a
# command, for as long as that command runs:
#
#   scripts/k8s-forward.sh                     # Ctrl+C to stop
#   scripts/k8s-forward.sh scripts/smoke.sh    # used by `make k8s-smoke`
set -euo pipefail

k() { kubectl --context kind-fintech --namespace fintech "$@"; }

k port-forward service/api 8081:8081 >/dev/null 2>&1 &
k port-forward service/query 8082:8082 >/dev/null 2>&1 &
k port-forward service/temporal 8233:8233 >/dev/null 2>&1 &
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

for _ in $(seq 1 60); do
  if curl -sf --max-time 2 localhost:8081/healthz >/dev/null && curl -sf --max-time 2 localhost:8082/healthz >/dev/null; then
    break
  fi
  sleep 1
done

if [ "$#" -gt 0 ]; then
  "$@"
else
  echo "Demo page: http://localhost:8082   Temporal: http://localhost:8233   (Ctrl+C to stop)"
  wait
fi
