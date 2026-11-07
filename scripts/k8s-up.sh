#!/usr/bin/env bash
# Runs the whole platform on a local kind cluster: creates the cluster if it
# doesn't exist, builds and loads the images, and applies the manifests in
# dependency order.
set -euo pipefail

CLUSTER=fintech
SERVICES=(api processor query worker)
k() { kubectl --context "kind-$CLUSTER" --namespace fintech "$@"; }

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"

for service in "${SERVICES[@]}"; do
  docker build -q -t "fintech/$service:dev" --build-arg "SERVICE=$service" .
  kind load docker-image --name "$CLUSTER" "fintech/$service:dev"
done
docker build -q -t fintech/migrate:dev -f deploy/k8s/migrate.Dockerfile .
kind load docker-image --name "$CLUSTER" fintech/migrate:dev

kubectl --context "kind-$CLUSTER" apply -f deploy/k8s/infra.yaml
k rollout status statefulset/postgres --timeout=300s
for deployment in kafka temporal otel-collector; do
  k rollout status "deployment/$deployment" --timeout=300s
done

# Job specs are immutable, so replace them on every run.
k delete job kafka-init migrate --ignore-not-found
kubectl --context "kind-$CLUSTER" apply -f deploy/k8s/setup-jobs.yaml
k wait --for=condition=complete job/kafka-init job/migrate --timeout=300s

kubectl --context "kind-$CLUSTER" apply -f deploy/k8s/app.yaml
# Pick up freshly built images, which all share the :dev tag.
for service in "${SERVICES[@]}"; do
  k rollout restart "deployment/$service"
done
for service in "${SERVICES[@]}"; do
  k rollout status "deployment/$service" --timeout=300s
done

k get pods
echo
echo "Running. 'make k8s-forward' exposes the demo page at http://localhost:8082."
