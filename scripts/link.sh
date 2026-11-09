#!/usr/bin/env bash
# Render + apply a ClusterLink CR.
#   link.sh <pri-to-sec|sec-to-pri> <none|ACTIVE|FAILOVER|PROMOTE> [--dry-run]
# pri-to-sec uses PRI2_CLUSTER_ID when present (re-protect, step G), else PRI_CLUSTER_ID.
# Note: on first creation CFK auto-mounts the link's jaasConfig secret into the destination brokers
# (rolling restart). The link reports ERROR "Could not read properties from file …" until that roll
# completes, then recovers by itself. Do NOT add the secret to Kafka spec.mountedSecrets (CFK rejects duplicates).
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh; source .secrets/ids.env
export LINK=$1 STATE=$2
case $LINK in
  pri-to-sec) export SRC_NS=$NS_PRI DEST_NS=$NS_SEC SRC_ID=${PRI2_CLUSTER_ID:-$PRI_CLUSTER_ID} ;;
  sec-to-pri) export SRC_NS=$NS_SEC DEST_NS=$NS_PRI SRC_ID=$SEC_CLUSTER_ID ;;
  *) echo "unknown link $LINK" >&2; exit 2 ;;
esac
if [[ $STATE == none ]]; then MIRRORS=" []"; else
  MIRRORS=""; for t in t1 t2 t3; do MIRRORS+=$'\n'"    - { name: $t, state: $STATE, replicationFactor: 3 }   # CFK defaults mirrors to RF 1 if unset"; done
fi
export MIRRORS KC_TOKEN_URL
y=$(envsubst '$LINK $DEST_NS $SRC_NS $SRC_ID $KC_TOKEN_URL $MIRRORS' < k8s/links/clusterlink.yaml.tmpl)
if [[ ${3:-} == --dry-run ]]; then echo "$y"; exit; fi
echo "$y" | kubectl apply -f -
echo "$(date -u +%FT%TZ) link $LINK src=$SRC_ID state=$STATE" >> evidence/timeline.txt
