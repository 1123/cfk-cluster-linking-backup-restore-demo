#!/usr/bin/env bash
# rpo.sh – data RPO: producer records ACKED by the primary but missing from the backup.
# Cluster Linking preserves offsets → an ACKED record (topic, partition, offset) is lost iff offset ≥ secondary end offset
# in audit-sec-after-D.json. Only ACKs before F-promote are considered (after promote new records get those offsets).
# Offset RPO: committed group offsets on primary vs. secondary at D.0.
source "$(dirname "$0")/lib.sh"
./scripts/fetch-evidence.sh >/dev/null
cutoff=$(awk '$2=="F-promote"{print $1}' evidence/timeline.txt | tail -1); cutoff=${cutoff:-9999}
dstart=$(awk '$2=="D-start"{print $1}' evidence/timeline.txt | tail -1)
jq -r '.topics | to_entries[] | .key as $t | .value | to_entries[] | "\($t) \(.key) \(.value.end)"' evidence/audit-sec-after-D.json > evidence/.sec-ends
{
echo "== Data RPO (cutoff for ACKs: < ${cutoff}; D-start ${dstart})"
cat evidence/clients/producer-t*.log | awk -F'\t' -v cutoff="$cutoff" -v dstart="$dstart" '
  BEGIN { while ((getline l < "evidence/.sec-ends") > 0) { split(l, a, " "); end[a[1] "-" a[2]] = a[3] } }
  $6=="ACKED" && $1 < cutoff {
    tp = $2 "-" $4; acked++
    if ($1 > dstart) late++
    if ($5 + 0 >= end[tp] + 0) { lost[tp]++; tot++; if (min=="" || $1<min) min=$1; if ($1>max) max=$1 }
    if ($1 > lastack) lastack = $1
  }
  END {
    printf "acked records considered: %d, last ACK: %s, ACKs after D-start: %d (expected 0 – isolation)\n", acked, lastack, late
    printf "LOST (acked, not in backup): %d records\n", tot
    for (tp in lost) printf "  %s: %d\n", tp, lost[tp]
    if (tot) printf "lost window: %s … %s\n", min, max
  }'
if [[ -f evidence/offsets-pri-D0.json && -f evidence/offsets-sec-D0.json ]]; then
  echo "== Offset RPO at D.0 (committed offsets primary − secondary, per group)"
  jq -c -n --slurpfile p evidence/offsets-pri-D0.json --slurpfile s evidence/offsets-sec-D0.json \
    '$p[0].groups | to_entries[] | .key as $g | {group: $g, records_behind: ([.value | to_entries[] | .value - ($s[0].groups[$g][.key] // 0)] | add)}'
fi
} | tee evidence/rpo.txt
