#!/usr/bin/env bash
# wait-offsets-synced.sh <link> [timeout_s] – demo-cg-* committed offsets on destination == source
# (lag 0 alone does not imply synced offsets; consumer.offset.sync.ms = 5 s)
source "$(dirname "$0")/lib.sh"
src=$(link_src "$1"); dst=$(link_dst "$1"); deadline=$((SECONDS + ${2:-300}))
while (( SECONDS < deadline )); do
  a=$(tb offsets "$src" | jq -S '.groups') || a=x
  b=$(tb offsets "$dst" | jq -S '.groups') || b=y
  if [[ $a == "$b" && $a != "{}" ]]; then echo "group offsets synced ($1):"; echo "$a" | jq -c .; exit 0; fi
  echo "  group offsets not yet equal ($1)"; sleep 10
done
echo "TIMEOUT: group offsets differ" >&2; diff <(echo "$a") <(echo "$b") || true; exit 1
