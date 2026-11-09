#!/usr/bin/env bash
# Phase 1 – demo PKI + generated credentials (local files only, idempotent; git-ignored)
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh
mkdir -p pki .secrets

if [[ ! -f pki/ca.pem ]]; then
  openssl req -x509 -newkey rsa:4096 -nodes -days 365 -keyout pki/ca-key.pem -out pki/ca.pem \
    -subj "/O=Kafka Demo/CN=Kafka Demo Root CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
fi

if [[ ! -f pki/keycloak.pem ]]; then
  openssl req -newkey rsa:2048 -nodes -keyout pki/keycloak-key.pem -out pki/keycloak.csr \
    -subj "/O=Kafka Demo/CN=${KC_HOST}"
  openssl x509 -req -in pki/keycloak.csr -CA pki/ca.pem -CAkey pki/ca-key.pem -CAcreateserial \
    -days 365 -sha256 -out pki/keycloak.pem -extfile <(printf '%s\n' \
      "basicConstraints=CA:FALSE" "keyUsage=digitalSignature,keyEncipherment" "extendedKeyUsage=serverAuth" \
      "subjectAltName=DNS:${KC_HOST},DNS:keycloak-service.keycloak.svc,DNS:keycloak-service,DNS:localhost")
  rm -f pki/keycloak.csr
fi

# JKS truststore for the broker JVM (OAuth token/JWKS calls to Keycloak – documented CFK limitation)
if [[ ! -f pki/truststore.jks ]]; then
  keytool -importcert -noprompt -alias demo-ca -file pki/ca.pem -keystore pki/truststore.jks \
    -storetype JKS -storepass mystorepassword
fi

if [[ ! -f .secrets/clients.env ]]; then
  gen() { openssl rand -hex 24; }
  cat > .secrets/clients.env <<EOT
KAFKA_BROKER_SECRET=$(gen)
CLUSTER_LINK_SECRET=$(gen)
PRODUCER_APP_SECRET=$(gen)
CONSUMER_APP_SECRET=$(gen)
KAFKA_ADMIN_SECRET=$(gen)
REST_ADMIN_PASSWORD=$(gen)
PASSWORD_ENCODER_SECRET=$(gen)
KC_ADMIN_PASSWORD=$(gen)
EOT
  chmod 600 .secrets/clients.env
fi
openssl verify -CAfile pki/ca.pem pki/keycloak.pem
echo "PKI and secrets ready."
