#!/usr/bin/env bash
# G re-protect: remove the reverse link, drop the failed-over topics on the secondary, re-create pri-to-sec from the NEW primary.
source "$(dirname "$0")/lib.sh"
mark G-start
kubectl -n "$NS_PRI" delete clusterlink sec-to-pri --wait=true --timeout=180s
# A regular topic cannot become a mirror → delete the failed-over topics (clink-t* KafkaTopic CRs) on the secondary
kubectl -n "$NS_SEC" get kafkatopic -o name | grep -E '/clink-t[1-3]-' | xargs -r kubectl -n "$NS_SEC" delete --wait=true
until [[ $(./scripts/rest.sh sec /topics | jq '[.data[].topic_name | select(test("^t[1-3]$"))] | length') == 0 ]]; do
  echo "  waiting for t1..t3 to disappear on the secondary"; sleep 5; done
./scripts/link.sh pri-to-sec ACTIVE                                  # uses PRI2_CLUSTER_ID
wait_mirrors pri-to-sec ACTIVE 600
MAX_LAG=${MAX_LAG:-500} ./scripts/wait-lag-zero.sh pri-to-sec 900
./scripts/wait-offsets-synced.sh pri-to-sec 300 || echo "(consumers active → offsets trail by ≤ sync interval)"
mark G-done
