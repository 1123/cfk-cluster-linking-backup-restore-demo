#!/usr/bin/env bash
# audit.sh <pri|sec> <label> – full read + per-partition digest → evidence/audit-<label>.json (also on the PVC)
source "$(dirname "$0")/lib.sh"
tb audit "$1" "$2" > "evidence/audit-$2.json"
jq -r '"audit \(.label) cluster=\(.cluster) id=\(.cluster_id)", (.topics | to_entries[] | .key as $t | .value | to_entries[]
  | "  \($t)-\(.key) start=\(.value.start) end=\(.value.end) count=\(.value.count) sha=\(.value.sha256[0:12])")' "evidence/audit-$2.json"
