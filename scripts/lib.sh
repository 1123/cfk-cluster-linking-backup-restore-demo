# shellcheck shell=bash
# Shared helpers for the runbook scripts (sourced, not executed).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source ./00-env.sh
[[ -f .secrets/ids.env ]] && source .secrets/ids.env
mkdir -p evidence
# Run the demo client image in the toolbox pod (offsets | audit); JSON on stdout, logs dropped.
tb() { kubectl -n "$NS_CLI" exec toolbox -- timeout 120 java -cp @/app/jib-classpath-file demo.Main "$@" 2>/dev/null; }
mark() { echo "$(date -u +%FT%TZ) $*" | tee -a evidence/timeline.txt; }
link_ns() { case $1 in pri-to-sec) echo "$NS_SEC";; sec-to-pri) echo "$NS_PRI";; *) echo "unknown link $1" >&2; return 2;; esac; }
link_src() { case $1 in pri-to-sec) echo pri;; sec-to-pri) echo sec;; esac; }
link_dst() { case $1 in pri-to-sec) echo sec;; sec-to-pri) echo pri;; esac; }
# wait_mirrors <link> <STATUS> [timeout_s] – all t1..t3 mirrors of the link report STATUS in the CR status
wait_mirrors() {
  local ns; ns=$(link_ns "$1"); local deadline=$((SECONDS + ${3:-600}))
  while (( SECONDS < deadline )); do
    local st; st=$(kubectl -n "$ns" get clusterlink "$1" -o json 2>/dev/null | jq -r --arg s "$2" \
      '[.status.mirrorTopics // {} | to_entries[] | select(.key|test("^t[1-3]$")) | select(.value.status==$s)] | length')
    [[ $st == 3 ]] && { echo "mirrors of $1: all $2"; return 0; }
    echo "  waiting for mirrors of $1 → $2 (now: $(kubectl -n "$ns" get clusterlink "$1" -o json 2>/dev/null | jq -c '[.status.state, (.status.mirrorTopics // {} | map_values(.status)), (.status.conditions // [] | map(select(.status=="False" and (.type|test("app-ready")))) | .[0].message // "" | .[0:160])]'))"
    sleep 15
  done
  echo "TIMEOUT waiting for mirrors of $1 → $2" >&2; return 1
}
wait_kafka_running() {
  local deadline=$((SECONDS + ${2:-1200}))
  until [[ $(kubectl -n "$1" get kafka kafka -o jsonpath='{.status.phase}' 2>/dev/null) == RUNNING ]]; do
    (( SECONDS > deadline )) && { echo "TIMEOUT waiting for kafka in $1" >&2; return 1; }; sleep 15
  done
}
