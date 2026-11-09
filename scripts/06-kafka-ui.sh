#!/usr/bin/env bash
# Optional: kafbat UI (read-only) for primary + secondary. Access: kubectl -n demo-clients port-forward svc/kafka-ui 8080:80
source "$(dirname "$0")/lib.sh"
set -a; source .secrets/clients.env; set +a
envsubst '$BOOT_PRI $BOOT_SEC $KC_TOKEN_URL $KAFKA_ADMIN_SECRET' < k8s/kafka-ui/config.yaml.tmpl |
  kubectl -n "$NS_CLI" create secret generic kafka-ui-config --from-file=config.yaml=/dev/stdin --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS_CLI" create secret generic kafka-ui-jvm-truststore --from-file=truststore.jks=pki/truststore.jks \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/kafka-ui/kafka-ui.yaml
kubectl -n "$NS_CLI" rollout restart deploy/kafka-ui >/dev/null      # pick up config changes
kubectl -n "$NS_CLI" rollout status deploy/kafka-ui --timeout=300s
