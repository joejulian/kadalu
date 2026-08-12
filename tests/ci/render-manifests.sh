#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
render_dir="${RUNNER_TEMP:-/tmp}/kadalu-rendered"
schema_base="https://raw.githubusercontent.com/yannh/kubernetes-json-schema"
schema_commit="c8f4e61c63bc529749125ac566bccc6986e08d45"
schema_template="{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}"
schema_resource="{{.ResourceKind}}{{.KindSuffix}}.json"
schema_location="${schema_base}/${schema_commit}/${schema_template}/${schema_resource}"

mkdir -p "${render_dir}"

python "${repo_root}/tests/ci/render_runtime_templates.py" \
  "${render_dir}/kadalu-runtime.yaml"

helm lint "${repo_root}/helm/kadalu" \
  --kube-version 1.36.1 \
  --set operator.enabled=true

for distro in kubernetes microk8s rke; do
  helm template kadalu "${repo_root}/helm/kadalu" \
    --include-crds \
    --kube-version 1.36.1 \
    --namespace ocean-crew \
    --set global.image.registry=registry.example.invalid \
    --set global.image.repository=heist-crew \
    --set global.kubernetesDistro="${distro}" \
    --set operator.enabled=true \
    >"${render_dir}/kadalu-${distro}.yaml"
done

helm template kadalu "${repo_root}/helm/kadalu" \
  --include-crds \
  --kube-version 1.36.1 \
  --namespace ocean-crew \
  --set global.image.registry=registry.example.invalid \
  --set global.image.repository=heist-crew \
  --set global.kubernetesDistro=openshift \
  --set operator.enabled=true \
  >"${render_dir}/kadalu-openshift.yaml"

# The OpenShift SCC is not part of the Kubernetes OpenAPI schema. Every other
# rendered object is validated strictly against the Kubernetes 1.36 schemas.
kubeconform \
  -kubernetes-version 1.36.0 \
  -schema-location "${schema_location}" \
  -skip CustomResourceDefinition \
  -strict \
  -summary \
  "${render_dir}/kadalu-kubernetes.yaml" \
  "${render_dir}/kadalu-microk8s.yaml" \
  "${render_dir}/kadalu-rke.yaml" \
  "${render_dir}/kadalu-runtime.yaml"

helm template kadalu "${repo_root}/helm/kadalu" \
  --include-crds \
  --kube-version 1.36.1 \
  --namespace ocean-crew \
  --set global.image.registry=registry.example.invalid \
  --set global.image.repository=heist-crew \
  --set global.kubernetesDistro=openshift \
  --set operator.enabled=true \
  --show-only charts/operator/templates/deployment.yaml \
  --show-only charts/operator/templates/rbac.yaml \
  --show-only charts/operator/templates/serviceaccount.yaml \
  | kubeconform \
      -kubernetes-version 1.36.0 \
      -schema-location "${schema_location}" \
      -strict \
      -summary

grep -q 'apiVersion: security.openshift.io/v1' \
  "${render_dir}/kadalu-openshift.yaml"

if grep -R -n 'v1beta1' "${render_dir}"; then
  echo "Legacy v1beta1 API found in rendered manifests" >&2
  exit 1
fi

echo "Rendered manifests are valid for Kubernetes 1.36"
