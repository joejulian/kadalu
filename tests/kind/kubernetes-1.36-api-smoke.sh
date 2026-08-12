#!/usr/bin/env bash

set -euo pipefail

KIND_NODE_IMAGE="kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"
cluster_name="kadalu-kubernetes-136"
repo_root="$(git rev-parse --show-toplevel)"
rendered_manifest="${RUNNER_TEMP:-/tmp}/kadalu-kind-1.36.yaml"
runtime_manifest="${RUNNER_TEMP:-/tmp}/kadalu-runtime-1.36.yaml"
export KUBECONFIG="${RUNNER_TEMP:-/tmp}/kadalu-kind-1.36.kubeconfig"

mkdir -p "${RUNNER_TEMP:-/tmp}"

cleanup() {
  kind delete cluster --name "${cluster_name}"
}
trap cleanup EXIT

kind create cluster \
  --name "${cluster_name}" \
  --image "${KIND_NODE_IMAGE}" \
  --kubeconfig "${KUBECONFIG}" \
  --wait 180s

kubectl version
kubectl wait --for=condition=Ready node --all --timeout=120s

helm template kadalu "${repo_root}/helm/kadalu" \
  --include-crds \
  --kube-version 1.36.1 \
  --namespace ocean-crew \
  --set global.image.pullPolicy=Never \
  --set global.image.registry=registry.example.invalid \
  --set global.image.repository=heist-crew \
  --set operator.enabled=true \
  >"${rendered_manifest}"

python "${repo_root}/tests/ci/render_runtime_templates.py" \
  "${runtime_manifest}"

if grep -n 'v1beta1' "${rendered_manifest}" "${runtime_manifest}"; then
  echo "Legacy v1beta1 API found in rendered manifests" >&2
  exit 1
fi

# Exercise the actual v1.36 API server without starting product workloads or
# pulling untrusted PR images. Server-side dry-run still performs discovery,
# defaulting, schema validation, admission, and authorization.
kubectl create namespace ocean-crew
kubectl apply --server-side --dry-run=server \
  --field-manager=kadalu-ci \
  -f "${rendered_manifest}"
kubectl apply --server-side --dry-run=server \
  --field-manager=kadalu-ci \
  -f "${runtime_manifest}"

kubectl api-resources --api-group=storage.k8s.io | grep -q '^csidrivers'
kubectl explain csidriver.spec --api-version=storage.k8s.io/v1 >/dev/null

echo "Kadalu manifests are accepted by the Kubernetes 1.36 API server"
