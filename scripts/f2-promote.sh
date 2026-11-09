#!/usr/bin/env bash
# F.4–F.7 promote mirrors on the new primary, wait for producers to resume, restart consumers, measure RTO.
source "$(dirname "$0")/lib.sh"
mark F-promote
./scripts/link.sh sec-to-pri PROMOTE
wait_mirrors sec-to-pri STOPPED 600
mark F-promoted
kubectl -n "$NS_PRI" get kafkatopic
echo "== waiting for producers to ACK again"
for t in t1 t2 t3; do
  until kubectl -n "$NS_CLI" logs deploy/producer-$t --tail=1 | grep -q "consecutive_failures=0"; do sleep 5; done
  echo "producer-$t: $(kubectl -n "$NS_CLI" logs deploy/producer-$t --tail=1 | cut -c32-)"
done
mark F-producers-acking
kubectl -n "$NS_CLI" scale deploy -l app=consumer --replicas=1
kubectl -n "$NS_CLI" wait --for=condition=Available deploy -l app=consumer --timeout=180s
sleep 20
for t in t1 t2 t3; do echo "== consumer-$t"; kubectl -n "$NS_CLI" exec toolbox -- grep ASSIGNED "/evidence/consumer-$t.log" | tail -6; done
mark F-done
start=$(awk '$2=="D-start"{print $1}' evidence/timeline.txt | tail -1); end=$(awk '$2=="F-done"{print $1}' evidence/timeline.txt | tail -1)
echo "RTO (D-start → F-done): $(( $(date -j -u -f %Y-%m-%dT%H:%M:%SZ "$end" +%s) - $(date -j -u -f %Y-%m-%dT%H:%M:%SZ "$start" +%s) )) s" | tee -a evidence/rpo.txt
