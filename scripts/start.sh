#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
alias k="kubectl --context=minikube"
# Start Minikube, skip if already running
if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  minikube start \
    --interactive=false \
    --kubernetes-version=v1.35.1 \
    --cpus=4 \
    --memory=8192 \
    --disk-size=30g \
    --addons=ingress
fi

# Bootstrap ArgoCD
k create namespace argocd
# --server-side required: ArgoCD CRDs exceed the last-applied-configuration annotation limit
k apply --server-side --force-conflicts -n argocd \
  -f https://raw.githubusercontent.com/argoproj/argo-cd/refs/tags/stable/manifests/install.yaml

# Wait for the ApplicationSet controller and API server before pushing the ApplicationSet
k wait --for=condition=available --timeout=180s -n argocd \
  deployment/argocd-applicationset-controller deployment/argocd-server

# Apply the ApplicationSet: ArgoCD takes over and deploys Airflow + RustFS from here
k apply -f "${REPO_ROOT}/cluster/application-set.yaml"