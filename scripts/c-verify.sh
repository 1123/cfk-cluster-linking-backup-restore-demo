#!/usr/bin/env bash
# Step C verification + pre-crash audit: mirror lag, group offset sync, client health, audit pri-before.
source "$(dirname "$0")/lib.sh"
MAX_LAG=${MAX_LAG:-500} ./scripts/wait-lag-zero.sh pri-to-sec 120     # producers running → small lag tolerated
./scripts/link-status.sh pri-to-sec | jq -c '{state, mirrors: (.mirrors | map_values(.status))}'
for d in producer-t1 producer-t2 producer-t3 consumer-t1 consumer-t2 consumer-t3; do
  echo "$d: $(kubectl -n "$NS_CLI" logs deploy/$d --tail=1 | cut -c32-)"; done
./scripts/audit.sh pri pri-before | head -4
