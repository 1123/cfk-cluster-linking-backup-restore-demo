#!/usr/bin/env bash
# link-status.sh <pri-to-sec|sec-to-pri>  – CR status: link state + per-mirror status
set -euo pipefail
cd "$(dirname "$0")/.."; source ./00-env.sh
case $1 in pri-to-sec) NS=$NS_SEC;; sec-to-pri) NS=$NS_PRI;; esac
kubectl -n "$NS" get clusterlink "$1" -o json | jq '{state: .status.state, sourceClusterID: .status.sourceKafkaClusterID,
  destClusterID: .status.destinationKafkaClusterID, mirrors: .status.mirrorTopics, conditions: [.status.conditions[]? | {type, status, message}]}'
