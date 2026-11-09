#!/usr/bin/env bash
# F.1–F.3 reverse link sec-to-pri: mirror the backup into the new primary, wait for data + offsets, verify identity.
# Note: creating the link makes CFK mount its credentials into the primary brokers → one rolling restart (several minutes);
# the link reports ERROR "Could not read properties from file …" until the roll is done.
source "$(dirname "$0")/lib.sh"
mark F-start
./scripts/link.sh sec-to-pri ACTIVE
wait_mirrors sec-to-pri ACTIVE 1500
./scripts/wait-lag-zero.sh sec-to-pri 900
./scripts/wait-offsets-synced.sh sec-to-pri 300
mark F-synced
./scripts/audit.sh pri pri-restored | head -4
./scripts/compare-audits.sh sec-after-D pri-restored
