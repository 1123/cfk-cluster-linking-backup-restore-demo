#!/usr/bin/env bash
# Build the demo client image with Jib and push it to Artifact Registry ($IMAGE from 00-env.sh).
# Bump the tag in 00-env.sh for every rebuild – pods use IfNotPresent and would keep a cached image.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./00-env.sh
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
(cd clients && mvn -q -B package jib:build -Dimage="$IMAGE")
echo "pushed $IMAGE"
