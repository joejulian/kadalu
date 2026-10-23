# Independent operator releases

An operator fix need not change the desired CSI node-plugin template. For a
compatibility backport, start from the operator release already in use and
backport the operator changes without unrelated CSI template changes. Keep the
CSI and server component ConfigMaps independent of the operator image version.

Push an `operator-X.Y.Z` or `operator-X.Y.Z-prerelease` tag to run the Operator
Release workflow. It first runs the complete pull-request validation workflow,
including the Kubernetes storage lifecycle checks. Only after those checks pass
does it publish `kadalu-operator:X.Y.Z` for amd64 and arm64. It does not publish
matching CSI or server versions, installers, or moving tags.

Commit `extras/operator-release-builder.json` with an immutable, already
published builder reference and its full source revision:

```json
{
  "image": "ghcr.io/example/kadalu-builder:SOURCE_COMMIT@sha256:MANIFEST_DIGEST",
  "revision": "SOURCE_COMMIT"
}
```

Replace the placeholders with the complete commit and digest. The workflow
requires a builder owned by the same repository owner, checks both architecture
manifests and source labels, and compares its Dockerfile and locked requirements
with the release source before reusing it. This avoids rebuilding an unchanged
Gluster dependency for every operator-only release. The published operator
records the tagged source revision. An existing version is preserved only when
its provenance matches that exact source; registry errors fail closed.

Before deployment, compare the desired CSI template with the template used by
the running node plugins. A real CSI template upgrade still requires the normal
OnDelete procedure: unpublish the node's volumes before replacing its plugin.
Do not relabel old pods or weaken the rollout gate to make a revision appear
current.
