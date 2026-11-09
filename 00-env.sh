# shellcheck shell=bash
# Environment-specific values (GCP project, owner label) live in the untracked .env.local – see env.local.example.
source "$(dirname "${BASH_SOURCE[0]}")/.env.local"
: "${PROJECT:?set PROJECT in .env.local}" "${OWNER:?set OWNER in .env.local}"
export REGION=europe-west3
export CLUSTER=cl-backup-restore
export LABELS=purpose=cl-backup-restore-demo,owner=${OWNER}
export CFK_CHART_VERSION=0.2038.1          # = CFK 3.4.0
export CP_VERSION=8.4.0
export CFK_INIT_VERSION=3.4.0
export KC_VERSION=26.8.2
export CNPG_CHART_VERSION=0.29.1
export NS_OP=confluent-operator NS_PRI=kafka-primary NS_SEC=kafka-secondary NS_KC=keycloak NS_CLI=demo-clients
export KC_HOST=keycloak-service.keycloak.svc.cluster.local
export KC_REALM_URL=https://${KC_HOST}:8443/realms/kafka-demo
export KC_TOKEN_URL=${KC_REALM_URL}/protocol/openid-connect/token
export KC_JWKS_URL=${KC_REALM_URL}/protocol/openid-connect/certs
export BOOT_PRI=kafka.${NS_PRI}.svc.cluster.local:9094
export BOOT_SEC=kafka.${NS_SEC}.svc.cluster.local:9094
export IMAGE=${REGION}-docker.pkg.dev/${PROJECT}/demo/kafka-demo-client:1.1
