#!/usr/bin/env bash
# Prints the Kafka cluster IDs and records them in .secrets/ids.env (PRI_CLUSTER_ID / SEC_CLUSTER_ID).
# Usage: scripts/cluster-id.sh [VAR_SUFFIX]   e.g. "2" writes PRI2_CLUSTER_ID after the primary was recreated.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh
sfx=${1:-}
pri=$(kubectl -n "$NS_PRI" get kafka kafka -o jsonpath='{.status.clusterID}' 2>/dev/null || true)
sec=$(kubectl -n "$NS_SEC" get kafka kafka -o jsonpath='{.status.clusterID}' 2>/dev/null || true)
touch .secrets/ids.env
upd() { grep -v "^$1=" .secrets/ids.env > .secrets/ids.tmp || true; echo "$1=$2" >> .secrets/ids.tmp; mv .secrets/ids.tmp .secrets/ids.env; }
[[ -n "$pri" ]] && upd "PRI${sfx}_CLUSTER_ID" "$pri"
[[ -z "$sfx" && -n "$sec" ]] && upd SEC_CLUSTER_ID "$sec"
cat .secrets/ids.env
