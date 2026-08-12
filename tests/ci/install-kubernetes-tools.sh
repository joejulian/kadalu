#!/usr/bin/env bash

set -euo pipefail

KUBECTL_VERSION="v1.36.3"
KUBECTL_SHA256="ebbd080e7c2e275093b55915722043257eb24004363e20acb3c4d71919f88336"
KIND_VERSION="v0.32.0"
KIND_SHA256="50030de23cf40a18505f20426f6a8506bedf13c6e509244bd1fa9463721b0f54"
HELM_VERSION="v4.2.3"
HELM_SHA256="e9b88b4ee95b18c706839c28d3a0220e5bc470e9cd9262410c90793c45ff8b7c"
KUBECONFORM_VERSION="v0.8.0"
KUBECONFORM_SHA256="9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883"

install_dir="${RUNNER_TEMP:-/tmp}/kadalu-ci-tools"
download_dir="${RUNNER_TEMP:-/tmp}/kadalu-ci-downloads"

mkdir -p "${install_dir}" "${download_dir}"

curl --fail --location --retry 5 --retry-all-errors \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
  --output "${download_dir}/kubectl"
echo "${KUBECTL_SHA256}  ${download_dir}/kubectl" | sha256sum --check --strict
install -m 0755 "${download_dir}/kubectl" "${install_dir}/kubectl"

curl --fail --location --retry 5 --retry-all-errors \
  "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64" \
  --output "${download_dir}/kind"
echo "${KIND_SHA256}  ${download_dir}/kind" | sha256sum --check --strict
install -m 0755 "${download_dir}/kind" "${install_dir}/kind"

helm_archive="helm-${HELM_VERSION}-linux-amd64.tar.gz"
curl --fail --location --retry 5 --retry-all-errors \
  "https://get.helm.sh/${helm_archive}" \
  --output "${download_dir}/${helm_archive}"
echo "${HELM_SHA256}  ${download_dir}/${helm_archive}" | sha256sum --check --strict
tar --extract --gzip --file "${download_dir}/${helm_archive}" \
  --directory "${download_dir}"
install -m 0755 "${download_dir}/linux-amd64/helm" "${install_dir}/helm"

kubeconform_archive="kubeconform-linux-amd64.tar.gz"
curl --fail --location --retry 5 --retry-all-errors \
  "https://github.com/yannh/kubeconform/releases/download/${KUBECONFORM_VERSION}/${kubeconform_archive}" \
  --output "${download_dir}/${kubeconform_archive}"
echo "${KUBECONFORM_SHA256}  ${download_dir}/${kubeconform_archive}" | \
  sha256sum --check --strict
tar --extract --gzip --file "${download_dir}/${kubeconform_archive}" \
  --directory "${download_dir}"
install -m 0755 "${download_dir}/kubeconform" "${install_dir}/kubeconform"

if [[ -n "${GITHUB_PATH:-}" ]]; then
  echo "${install_dir}" >>"${GITHUB_PATH}"
fi

echo "Installed Kubernetes CI tools in ${install_dir}"
