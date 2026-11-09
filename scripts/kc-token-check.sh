#!/usr/bin/env bash
# Gate 2: obtain a token for every demo client from inside the cluster and print the relevant claims.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh
set -a; source .secrets/clients.env; set +a
POD=kc-token-check
kubectl -n "$NS_KC" delete pod "$POD" --ignore-not-found --wait >/dev/null
kubectl -n "$NS_KC" run "$POD" --restart=Never --image=alpine:3.22 \
  --overrides='{"spec":{"volumes":[{"name":"ca","secret":{"secretName":"demo-ca"}}],"containers":[{"name":"kc-token-check","image":"alpine:3.22","command":["sleep","600"],"volumeMounts":[{"name":"ca","mountPath":"/ca"}]}]}}' >/dev/null
trap 'kubectl -n "$NS_KC" delete pod "$POD" --wait=false >/dev/null 2>&1 || true' EXIT
kubectl -n "$NS_KC" wait --for=condition=Ready pod/"$POD" --timeout=120s >/dev/null
kubectl -n "$NS_KC" exec -i "$POD" -- sh -s <<SCRIPT
set -e
apk add -q --no-cache curl jq >/dev/null
for pair in kafka-broker:$KAFKA_BROKER_SECRET cluster-link:$CLUSTER_LINK_SECRET producer-app:$PRODUCER_APP_SECRET consumer-app:$CONSUMER_APP_SECRET kafka-admin:$KAFKA_ADMIN_SECRET; do
  id=\${pair%%:*}; secret=\${pair#*:}
  tok=\$(curl -sf --cacert /ca/ca.pem -d grant_type=client_credentials -u "\$id:\$secret" "$KC_TOKEN_URL" | jq -r .access_token)
  echo "\$tok" | cut -d. -f2 | tr '_-' '/+' | awk '{l=length(\$0)%4; if(l==2)print \$0"=="; else if(l==3)print \$0"="; else print}' \
    | base64 -d 2>/dev/null | jq -c --arg id "\$id" '{client:\$id, iss, aud, client_id, azp, exp_in:(.exp-.iat)}'
done
echo "-- negative test (wrong secret):"
curl -s -o /dev/null -w '%{http_code}\n' --cacert /ca/ca.pem -d grant_type=client_credentials -u "producer-app:wrong" "$KC_TOKEN_URL"
SCRIPT
