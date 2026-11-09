#!/usr/bin/env bash
# rest.sh <pri|sec> <path-after-/clusters/<id>> [curl args…] – call the embedded Admin REST API via port-forward
set -euo pipefail
cd "$(dirname "$0")/.."; source ./00-env.sh; source .secrets/ids.env; source .secrets/clients.env
case $1 in pri) NS=$NS_PRI ID=${PRI2_CLUSTER_ID:-$PRI_CLUSTER_ID} PORT=28090;; sec) NS=$NS_SEC ID=$SEC_CLUSTER_ID PORT=18090;; esac
kubectl -n "$NS" port-forward svc/kafka $PORT:8090 >/dev/null 2>&1 & PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 20); do nc -z localhost $PORT 2>/dev/null && break; sleep 0.5; done
# NOTE: embedded REST currently plain HTTP (see kafka.yaml services.kafkaRest)
curl -s -u "rest-admin:$REST_ADMIN_PASSWORD" "http://localhost:$PORT/kafka/v3/clusters/$ID$2" "${@:3}"
