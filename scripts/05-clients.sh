#!/usr/bin/env bash
# Phase 4: secrets + client workloads in ns demo-clients (producers/consumers stay at replicas 0 until step C).
# WARNING: re-running this resets producer/consumer Deployments to replicas 0 (as defined in the template).
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh
set -a; source .secrets/clients.env; set +a
kubectl get ns "$NS_CLI" >/dev/null 2>&1 || kubectl create ns "$NS_CLI"
apply_secret() { kubectl -n "$NS_CLI" create secret generic "$@" --dry-run=client -o yaml | kubectl apply -f -; }
apply_secret demo-ca        --from-file=ca.pem=pki/ca.pem
apply_secret creds-producer --from-literal=CLIENT_ID=producer-app --from-literal=CLIENT_SECRET="$PRODUCER_APP_SECRET"
apply_secret creds-consumer --from-literal=CLIENT_ID=consumer-app --from-literal=CLIENT_SECRET="$CONSUMER_APP_SECRET"
apply_secret creds-admin    --from-literal=CLIENT_ID=kafka-admin  --from-literal=CLIENT_SECRET="$KAFKA_ADMIN_SECRET"
envsubst '$IMAGE' < k8s/clients/clients.yaml.tmpl | kubectl apply -f -
kubectl -n "$NS_CLI" wait --for=condition=Ready pod/toolbox --timeout=300s
kubectl -n "$NS_CLI" get deploy,pod,pvc
