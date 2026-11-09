#!/usr/bin/env bash
# Phase 2 – Keycloak (Keycloak Operator + CloudNativePG) in namespace keycloak
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh
set -a; source .secrets/clients.env; set +a

echo "== CloudNativePG operator"
helm repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null
helm repo update cnpg >/dev/null
helm upgrade --install cnpg cnpg/cloudnative-pg --version "$CNPG_CHART_VERSION" \
  -n cnpg-system --create-namespace --wait

echo "== Secrets"
kubectl -n "$NS_KC" create secret tls keycloak-tls --cert=pki/keycloak.pem --key=pki/keycloak-key.pem \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS_KC" create secret generic kc-client-secrets \
  --from-literal=KAFKA_BROKER_SECRET="$KAFKA_BROKER_SECRET" \
  --from-literal=CLUSTER_LINK_SECRET="$CLUSTER_LINK_SECRET" \
  --from-literal=PRODUCER_APP_SECRET="$PRODUCER_APP_SECRET" \
  --from-literal=CONSUMER_APP_SECRET="$CONSUMER_APP_SECRET" \
  --from-literal=KAFKA_ADMIN_SECRET="$KAFKA_ADMIN_SECRET" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS_KC" create secret generic demo-ca --from-file=ca.pem=pki/ca.pem \
  --dry-run=client -o yaml | kubectl apply -f -

echo "== Keycloak operator $KC_VERSION"
KCR=https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/${KC_VERSION}/kubernetes
for crd in keycloaks keycloakrealmimports keycloakoidcclients keycloaksamlclients; do   # operator ≥ 26.6 watches all four
  kubectl apply --server-side -f "$KCR/${crd}.k8s.keycloak.org-v1.yml"
done
kubectl -n "$NS_KC" apply -f "$KCR/kubernetes.yml"
kubectl -n "$NS_KC" rollout status deploy/keycloak-operator --timeout=5m

echo "== Postgres"
kubectl apply -f k8s/keycloak/cnpg-cluster.yaml
kubectl -n "$NS_KC" wait cluster.postgresql.cnpg.io/kc-db --for=condition=Ready --timeout=10m

echo "== Keycloak"
kubectl apply -f k8s/keycloak/keycloak.yaml
kubectl -n "$NS_KC" wait keycloak/keycloak --for=condition=Ready --timeout=15m

echo "== Realm import"
kubectl apply -f k8s/keycloak/realm-import.yaml
kubectl -n "$NS_KC" wait keycloakrealmimport/kafka-demo --for=condition=Done --timeout=10m

echo "== Gate 2 – client-credentials tokens"
scripts/kc-token-check.sh
