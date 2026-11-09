#!/usr/bin/env bash
# D.2 incident response: stop consumers (groups must be inactive on the destination for offset restore). Producers keep running.
source "$(dirname "$0")/lib.sh"
kubectl -n "$NS_CLI" scale deploy -l app=consumer --replicas=0
kubectl -n "$NS_CLI" wait --for=delete pod -l app=consumer --timeout=120s
mark D-consumers-stopped
