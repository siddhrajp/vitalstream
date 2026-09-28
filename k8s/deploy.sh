#!/bin/sh
# Deploy VitalStream to the current Kubernetes context (Docker Desktop: "docker-desktop").
#
#   k8s/deploy.sh           apply the manifests (images must already be built)
#   k8s/deploy.sh --build   build the three service images first
#
# Stop the docker-compose stack first (docker compose stop): both use the same localhost ports
# (8080, 8082, 8180, 9090) and compete for Docker's memory.
set -e
cd "$(dirname "$0")/.."

if [ "$1" = "--build" ]; then
  for service in api-service vitals-processor vitals-query; do
    echo "== building vitalstream/$service:dev"
    docker build -q --build-arg SERVICE=$service -t vitalstream/$service:dev .
  done
fi

echo "== namespace"
kubectl apply -f k8s/namespace.yaml

# The realm is generated from keycloak/realm-vitalstream.json here, not by kustomize, because kustomize
# refuses to read files outside the k8s/ folder. "create --dry-run | apply" creates or updates it.
echo "== keycloak realm ConfigMap"
kubectl create configmap keycloak-realm -n vitalstream \
  --from-file=realm-vitalstream.json=keycloak/realm-vitalstream.json \
  --dry-run=client -o yaml | kubectl apply -f -

echo "== everything else"
kubectl apply -k k8s

echo
echo "Watch pods start:  kubectl get pods -n vitalstream -w"
echo "Services start in any order; Kubernetes restarts any that start before their dependencies are ready."
