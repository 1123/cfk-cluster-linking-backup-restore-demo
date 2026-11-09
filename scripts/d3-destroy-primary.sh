#!/usr/bin/env bash
# D.3 destroy the primary cluster incl. storage. The namespace (secrets, KafkaRestClass, NetworkPolicy) stays.
source "$(dirname "$0")/lib.sh"
mark D3-destroy-start
kubectl get pv -o json | jq -r --arg ns "$NS_PRI" '.items[] | select(.spec.claimRef.namespace==$ns) | .metadata.name' > evidence/d3-pvs.txt
echo "PVs of the primary: $(tr '\n' ' ' < evidence/d3-pvs.txt)"
# 1. KafkaTopic CRs first – otherwise CFK re-creates t1..t3 as regular topics on the new cluster (blocks mirrors in F).
#    CFK cannot reach the isolated brokers → strip finalizers.
kubectl -n "$NS_PRI" delete kafkatopic --all --wait=false
sleep 20
for t in $(kubectl -n "$NS_PRI" get kafkatopic -o name); do kubectl -n "$NS_PRI" patch "$t" --type=merge -p '{"metadata":{"finalizers":[]}}'; done
# 2. Cluster CRs (StatefulSets, pods, services go with them); finalizer fallback
kubectl -n "$NS_PRI" delete kafka/kafka kraftcontroller/kraftcontroller --wait=false
for _ in $(seq 1 18); do
  [[ -z $(kubectl -n "$NS_PRI" get kafka,kraftcontroller -o name 2>/dev/null) ]] && break; sleep 10; done
for r in $(kubectl -n "$NS_PRI" get kafka,kraftcontroller -o name 2>/dev/null); do
  echo "stripping finalizers of $r"; kubectl -n "$NS_PRI" patch "$r" --type=merge -p '{"metadata":{"finalizers":[]}}'; done
kubectl -n "$NS_PRI" wait --for=delete pod --all --timeout=300s
# 3. Storage: delete PVCs; PVs with reclaim Retain are switched to Delete and removed explicitly
kubectl -n "$NS_PRI" delete pvc --all --wait=true
for pv in $(cat evidence/d3-pvs.txt); do
  if kubectl get pv "$pv" >/dev/null 2>&1; then
    kubectl patch pv "$pv" -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'; kubectl delete pv "$pv" --wait=false; fi
done
echo "== verify"
kubectl -n "$NS_PRI" get kafka,kraftcontroller,kafkatopic,pvc,pod 2>&1
for _ in $(seq 1 18); do
  left=$(for pv in $(cat evidence/d3-pvs.txt); do gcloud compute disks list --project "$PROJECT" --filter="name=$pv" --format="value(name)"; done)
  [[ -z $left ]] && break; echo "  disks still being deleted: $left"; sleep 10; done
[[ -z $left ]] && echo "OK: all primary disks deleted" || { echo "!! disks left: $left" >&2; exit 1; }
mark D3-destroyed
