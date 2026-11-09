#!/usr/bin/env bash
# E recreate an EMPTY primary (same names, new storage, new cluster ID). Topics manifest is NOT applied.
source "$(dirname "$0")/lib.sh"
mark E-start
kubectl delete -f k8s/netpol-isolate-primary.yaml --ignore-not-found
kubectl apply -f k8s/kafka-primary/
sleep 30; wait_kafka_running "$NS_PRI" 1200
./scripts/cluster-id.sh 2 >/dev/null; source .secrets/ids.env
echo "PRI_CLUSTER_ID=$PRI_CLUSTER_ID  PRI2_CLUSTER_ID=$PRI2_CLUSTER_ID"
[[ $PRI2_CLUSTER_ID != "$PRI_CLUSTER_ID" ]] || { echo "!! cluster ID unchanged" >&2; exit 1; }
new_pvs=$(kubectl get pv -o json | jq -r --arg ns "$NS_PRI" '.items[] | select(.spec.claimRef.namespace==$ns) | .metadata.name')
grep -qxF -f evidence/d3-pvs.txt <<<"$new_pvs" && { echo "!! old PV reused" >&2; exit 1; } || echo "OK: new PVs only"
./scripts/offsets.sh pri E | jq -c '{end_offsets, groups}'           # expect UnknownTopicOrPartition for t1..t3, no groups
mark E-done
