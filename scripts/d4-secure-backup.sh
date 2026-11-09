#!/usr/bin/env bash
# D.4 secure the backup on the secondary: FAILOVER mirrors (works without the source), drop the old link, audit, RPO.
source "$(dirname "$0")/lib.sh"
kubectl -n "$NS_SEC" get clusterlink pri-to-sec -o yaml > evidence/pri-to-sec-final.yaml
mark D4-failover
./scripts/link.sh pri-to-sec FAILOVER
# Fallback if CFK cannot reconcile (source dead): ./scripts/rest.sh sec /links/pri-to-sec/mirrors:failover -X POST \
#   -H 'Content-Type: application/json' -d '{"mirror_topic_names":["t1","t2","t3"]}'
wait_mirrors pri-to-sec STOPPED 600
kubectl -n "$NS_SEC" get kafkatopic                                   # CFK created clink-t*-<uid> CRs
kubectl -n "$NS_SEC" delete clusterlink pri-to-sec --wait=true --timeout=180s   # verified: failed-over topics survive
mark D4-link-deleted
./scripts/audit.sh sec sec-after-D | head -4
./scripts/rpo.sh
