#!/usr/bin/env bash
# wait-lag-zero.sh <link> [timeout_s] – until destination end offsets of t1..t3 == source (MAX_LAG records tolerance, default 0)
source "$(dirname "$0")/lib.sh"
src=$(link_src "$1"); dst=$(link_dst "$1"); deadline=$((SECONDS + ${2:-900})); max=${MAX_LAG:-0}
while (( SECONDS < deadline )); do
  a=$(tb offsets "$src" | jq -c '.end_offsets') || a='{}'
  b=$(tb offsets "$dst" | jq -c '.end_offsets') || b='{}'
  lag=$(jq -n --argjson a "$a" --argjson b "$b" '[ $a | to_entries[] | .key as $t | .value | to_entries[]
        | (.value - (($b[$t] // {})[.key] // 0)) ] | if length==0 then 999999 else (map(if . < 0 then 0 else . end) | add) end')
  echo "  $1 total mirror lag: $lag records"
  (( lag <= max )) && { echo "lag OK ($1)"; exit 0; }
  sleep 10
done
echo "TIMEOUT: lag not zero" >&2; exit 1
