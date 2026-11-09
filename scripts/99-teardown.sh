#!/usr/bin/env bash
# Delete everything: GKE cluster, leftover disks, Artifact Registry repo.
source "$(dirname "$0")/lib.sh"
read -r -p "Delete GKE cluster $CLUSTER, its disks and the Artifact Registry repo 'demo' in $PROJECT? [yes/N] " a
[[ $a == yes ]] || exit 1
# Remember this cluster's PD disks (only these are cleaned up afterwards – the project is shared)
kubectl get pv -o json | jq -r '.items[] | .spec.csi.volumeHandle // empty' > evidence/teardown-disks.txt   # projects/P/zones/Z/disks/NAME
kubectl delete ns "$NS_PRI" "$NS_SEC" --wait=true --timeout=10m || true
helm uninstall confluent-operator -n "$NS_OP" || true
gcloud container clusters delete "$CLUSTER" --region "$REGION" --project "$PROJECT" --quiet
while IFS=/ read -r _ _ _ zone _ name; do
  [[ -n $name ]] || continue
  if gcloud compute disks describe "$name" --zone "$zone" --project "$PROJECT" >/dev/null 2>&1; then
    echo "leftover disk $name ($zone)"; gcloud compute disks delete "$name" --zone "$zone" --project "$PROJECT" --quiet || true; fi
done < evidence/teardown-disks.txt
gcloud artifacts repositories delete demo --location="$REGION" --project "$PROJECT" --quiet
