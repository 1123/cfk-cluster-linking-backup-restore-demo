#!/usr/bin/env bash
# compare-audits.sh <labelA> <labelB> – per partition start/end/count/sha256 must be identical
source "$(dirname "$0")/lib.sh"
norm() { jq -S '.topics | map_values(map_values({start, end, count, sha256}))' "evidence/audit-$1.json"; }
if diff <(norm "$1") <(norm "$2") > "evidence/compare-$1-vs-$2.diff"; then
  echo "IDENTICAL: $1 == $2 ($(jq '[.topics[][]] | length' "evidence/audit-$1.json") partitions, $(jq '[.topics[][].count] | add' "evidence/audit-$1.json") records)"
else
  echo "DIFFERENT: see evidence/compare-$1-vs-$2.diff"; cat "evidence/compare-$1-vs-$2.diff"; exit 1
fi
