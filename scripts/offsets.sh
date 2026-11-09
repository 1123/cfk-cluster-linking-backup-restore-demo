#!/usr/bin/env bash
# offsets.sh <pri|sec> [label] – end offsets of t1..t3 + demo-cg-* group offsets (saved as evidence/offsets-<c>-<label>.json)
source "$(dirname "$0")/lib.sh"
out=$(tb offsets "$1")
[[ -n ${2:-} ]] && echo "$out" > "evidence/offsets-$1-$2.json"
echo "$out" | jq -c '{cluster, cluster_id, end_offsets, groups}'
