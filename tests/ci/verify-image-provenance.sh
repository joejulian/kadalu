#!/usr/bin/env bash

set -euo pipefail

if (( $# != 3 )); then
  echo "usage: $0 IMAGE EXPECTED_REVISION EXPECTED_SOURCE" >&2
  exit 2
fi

image=$1
expected_revision=$2
expected_source=$3
image_json="$(docker buildx imagetools inspect \
  --format '{{json .Image}}' "${image}")"

for platform in linux/amd64 linux/arm64; do
  revision="$(jq -er --arg platform "${platform}" \
    '.[$platform].config.Labels["org.opencontainers.image.revision"] // empty' \
    <<<"${image_json}")" || {
      echo "${image} has no revision label for ${platform}" >&2
      exit 1
    }
  source="$(jq -er --arg platform "${platform}" \
    '.[$platform].config.Labels["org.opencontainers.image.source"] // empty' \
    <<<"${image_json}")" || {
      echo "${image} has no source label for ${platform}" >&2
      exit 1
    }

  if [[ "${revision}" != "${expected_revision}" ]]; then
    echo "${image} ${platform} belongs to revision ${revision}, not ${expected_revision}" >&2
    exit 1
  fi
  if [[ "${source}" != "${expected_source}" ]]; then
    echo "${image} ${platform} belongs to source ${source}, not ${expected_source}" >&2
    exit 1
  fi
done

echo "Verified ${image} provenance for amd64 and arm64"
