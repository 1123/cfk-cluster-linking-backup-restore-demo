#!/usr/bin/env bash
# D.0 baseline + D.1 hard crash of the primary ("lights out"): deny-all NetworkPolicy, then force-kill all pods.
source "$(dirname "$0")/lib.sh"
echo "== D.0 baseline offsets"
./scripts/offsets.sh pri D0 >/dev/null & ./scripts/offsets.sh sec D0 >/dev/null & wait
mark D-start
kubectl apply -f k8s/netpol-isolate-primary.yaml
sleep 5                                                     # let Dataplane V2 program the policy before killing pods
kubectl -n "$NS_PRI" delete pod --all --grace-period=0 --force 2>&1 | grep -v "^Warning" || true
mark D-pods-killed
echo "== verify isolation (new connections to the primary must fail)"
sleep 20
if tb offsets pri >/dev/null; then echo "!! primary still reachable – isolation NOT effective" >&2; exit 1; fi
echo "OK: primary unreachable"
sleep 20
for d in producer-t1 consumer-t1; do echo "$d: $(kubectl -n "$NS_CLI" logs deploy/$d --tail=1 | cut -c32-)"; done
kubectl -n "$NS_SEC" get clusterlink pri-to-sec -o json | jq -c '{state: .status.state, mirrors: (.status.mirrorTopics | map_values(.status))}'
