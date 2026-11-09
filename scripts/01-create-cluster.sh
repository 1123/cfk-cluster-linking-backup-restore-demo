#!/usr/bin/env bash
# Phase 0 – one regional GKE cluster (adapted from gcp-cp-200MB-per-sec-load-test/scripts/01-create-cluster.sh)
# Storage: GKE built-in StorageClass premium-rwo (pd-ssd, reclaimPolicy Delete, WaitForFirstConsumer).
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh
ZONES=${REGION}-a,${REGION}-b,${REGION}-c

gcloud config set project "$PROJECT"
gcloud services enable container.googleapis.com artifactregistry.googleapis.com

if ! gcloud container clusters describe "$CLUSTER" --region "$REGION" >/dev/null 2>&1; then
  gcloud container clusters create "$CLUSTER" --region "$REGION" --node-locations "$ZONES" \
    --num-nodes 1 --machine-type n2-standard-4 --disk-size 50 \
    --enable-dataplane-v2 \
    --labels "$LABELS" --release-channel regular --quiet
fi

gcloud container node-pools describe kafka --cluster "$CLUSTER" --region "$REGION" >/dev/null 2>&1 ||
  gcloud container node-pools create kafka --cluster "$CLUSTER" --region "$REGION" --node-locations "$ZONES" \
    --num-nodes 1 --machine-type n2-standard-8 --disk-type pd-ssd --disk-size 100 \
    --node-labels role=kafka --node-taints dedicated=kafka:NoSchedule --quiet

gcloud container node-pools describe clients --cluster "$CLUSTER" --region "$REGION" >/dev/null 2>&1 ||
  gcloud container node-pools create clients --cluster "$CLUSTER" --region "$REGION" --node-locations "$ZONES" \
    --num-nodes 1 --machine-type n2-standard-4 --node-labels role=client --quiet

gcloud container clusters get-credentials "$CLUSTER" --region "$REGION"
gcloud artifacts repositories describe demo --location="$REGION" >/dev/null 2>&1 ||
  gcloud artifacts repositories create demo --repository-format=docker --location="$REGION" --labels "$LABELS"
# Nodes run as the default compute SA, which has no Artifact Registry read access in this project → grant it on this repo only.
NODE_SA="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')-compute@developer.gserviceaccount.com"
gcloud artifacts repositories add-iam-policy-binding demo --location="$REGION" \
  --member "serviceAccount:$NODE_SA" --role roles/artifactregistry.reader --condition=None >/dev/null

for ns in $NS_OP $NS_PRI $NS_SEC $NS_KC $NS_CLI; do
  kubectl create ns "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

echo "== Gate 0"
kubectl get nodes -L role,topology.kubernetes.io/zone
kubectl get storageclass premium-rwo
kubectl -n kube-system get pods -l k8s-app=cilium --no-headers | awk '{print $1, $3}'
