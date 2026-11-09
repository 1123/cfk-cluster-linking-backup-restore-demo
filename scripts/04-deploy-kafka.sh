#!/usr/bin/env bash
# Phase 3 – CFK operator + both Confluent Platform clusters.
#   scripts/04-deploy-kafka.sh                    full deployment (operator, secrets, both clusters, topics on primary)
#   scripts/04-deploy-kafka.sh --secrets-only NS  only (re)create the secrets in namespace NS
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh
set -a; source .secrets/clients.env; set +a

create_secrets() {
  local ns=$1 tmp; tmp=$(mktemp -d)
  printf 'clientId=kafka-broker\nclientSecret=%s\n' "$KAFKA_BROKER_SECRET" > "$tmp/broker.txt"
  printf 'clientId=cluster-link\nclientSecret=%s\n' "$CLUSTER_LINK_SECRET" > "$tmp/link.txt"
  printf 'password=%s\n' "$PASSWORD_ENCODER_SECRET" > "$tmp/encoder.txt"
  printf 'rest-admin: %s,Administrators\n' "$REST_ADMIN_PASSWORD" > "$tmp/rest-server.txt"
  printf 'username=rest-admin\npassword=%s\n' "$REST_ADMIN_PASSWORD" > "$tmp/rest-client.txt"
  k() { kubectl -n "$ns" create secret "$@" --dry-run=client -o yaml | kubectl apply -f - >/dev/null; }
  k tls ca-pair-sslcerts --cert=pki/ca.pem --key=pki/ca-key.pem
  k generic demo-ca-pem --from-file=cacerts.pem=pki/ca.pem
  k generic jvm-truststore --from-file=truststore.jks=pki/truststore.jks
  k generic oauth-jaas-broker --from-file=oauth.txt="$tmp/broker.txt"
  k generic oauth-jaas-cluster-link --from-file=oauth.txt="$tmp/link.txt"
  k generic password-encoder-secret --from-file=password-encoder.txt="$tmp/encoder.txt"
  k generic rest-credentials --from-file=basic.txt="$tmp/rest-server.txt"
  k generic rest-client-credentials --from-file=basic.txt="$tmp/rest-client.txt"
  rm -rf "$tmp"
  echo "secrets created in $ns"
}

if [[ "${1:-}" == "--secrets-only" ]]; then create_secrets "$2"; exit 0; fi

echo "== CFK $CFK_CHART_VERSION (one operator, watches both Kafka namespaces)"
helm repo add confluentinc https://packages.confluent.io/helm >/dev/null
helm repo update confluentinc >/dev/null
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes --version "$CFK_CHART_VERSION" \
  -n "$NS_OP" --set namespaced=true --set "namespaceList={$NS_PRI,$NS_SEC}" --wait

for ns in $NS_PRI $NS_SEC; do create_secrets "$ns"; done

echo "== Deploy clusters"
for ns in $NS_PRI $NS_SEC; do kubectl apply -f "k8s/$ns/"; done
for ns in $NS_PRI $NS_SEC; do
  kubectl -n "$ns" wait kraftcontroller/kraftcontroller --for=jsonpath='{.status.phase}'=RUNNING --timeout=20m
  kubectl -n "$ns" wait kafka/kafka --for=jsonpath='{.status.phase}'=RUNNING --timeout=20m
done

scripts/cluster-id.sh
for ns in $NS_PRI $NS_SEC; do kubectl -n "$ns" get pod -o wide; done
