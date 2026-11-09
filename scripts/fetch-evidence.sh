#!/usr/bin/env bash
# fetch-evidence.sh – copy the client evidence PVC (/evidence in the toolbox) to ./evidence/clients/
source "$(dirname "$0")/lib.sh"
mkdir -p evidence/clients
kubectl -n "$NS_CLI" exec toolbox -- tar cf - -C /evidence --exclude=lost+found . | tar xf - -C evidence/clients
ls -l evidence/clients
