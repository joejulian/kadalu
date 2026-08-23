#!/usr/bin/env bash

set -euo pipefail

KIND_NODE_IMAGE="kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"
BUSYBOX_IMAGE="docker.io/library/busybox:1.37.0@sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0"
IMAGE_REGISTRY=${IMAGE_REGISTRY:-docker.io}
IMAGE_REPOSITORY=${IMAGE_REPOSITORY:-kadalu-runtime}
IMAGE_TAG=${IMAGE_TAG:-test}
KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:-kadalu-kubernetes-136-storage}
KADALU_NAMESPACE=${KADALU_NAMESPACE:-ocean-crew}
repo_root="$(git rev-parse --show-toplevel)"
temp_root=${RUNNER_TEMP:-/tmp}
work_dir="$(mktemp -d "${temp_root}/kadalu-kind-1.36.XXXXXX")"
brick_dir="${work_dir}/bellagio-brick"
kind_config="${work_dir}/kind.yaml"
export KUBECONFIG="${work_dir}/kubeconfig"

mkdir -p "${brick_dir}"

diagnostics() {
  kubectl get nodes -o wide || true
  kubectl get kadalustorages --all-namespaces -o yaml || true
  kubectl get pods,pvc,pv,storageclass --all-namespaces -o wide || true
  kubectl get events --all-namespaces \
    --sort-by=.metadata.creationTimestamp | tail -200 || true
  kubectl logs -n "${KADALU_NAMESPACE}" \
    --all-containers --prefix --tail=200 \
    -l app.kubernetes.io/part-of=kadalu || true
  kubectl logs -n "${KADALU_NAMESPACE}" \
    --all-containers --prefix --tail=200 \
    -l name=kadalu || true
}

cleanup() {
  status=$?
  if (( status != 0 )); then
    diagnostics
  fi
  kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
  case "${work_dir}" in
    "${temp_root}"/kadalu-kind-1.36.*) sudo find "${work_dir}" -depth -delete ;;
    *) echo "Refusing to remove unexpected work directory: ${work_dir}" >&2 ;;
  esac
  exit "${status}"
}
trap cleanup EXIT

for image in kadalu-operator kadalu-csi kadalu-server; do
  docker image inspect \
    "${IMAGE_REGISTRY}/${IMAGE_REPOSITORY}/${image}:${IMAGE_TAG}" >/dev/null
done

cat >"${kind_config}" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: ${brick_dir}
        containerPath: /var/lib/kadalu-ci/bellagio-brick
EOF

kind create cluster \
  --name "${KIND_CLUSTER_NAME}" \
  --image "${KIND_NODE_IMAGE}" \
  --config "${kind_config}" \
  --kubeconfig "${KUBECONFIG}" \
  --wait 180s

for image in kadalu-operator kadalu-csi kadalu-server; do
  kind load docker-image \
    "${IMAGE_REGISTRY}/${IMAGE_REPOSITORY}/${image}:${IMAGE_TAG}" \
    --name "${KIND_CLUSTER_NAME}"
done

helm upgrade --install kadalu "${repo_root}/helm/kadalu" \
  --namespace "${KADALU_NAMESPACE}" \
  --create-namespace \
  --set operator.enabled=true \
  --set global.image.registry="${IMAGE_REGISTRY}" \
  --set global.image.repository="${IMAGE_REPOSITORY}" \
  --set global.image.pullPolicy=Never \
  --set-string global.kadaluVersion="${IMAGE_TAG}" \
  --wait \
  --timeout 3m

kubectl wait -n "${KADALU_NAMESPACE}" \
  --for=condition=Available deployment/operator --timeout=120s

cat <<EOF | kubectl apply -n "${KADALU_NAMESPACE}" -f -
apiVersion: kadalu-operator.storage/v1alpha1
kind: KadaluStorage
metadata:
  name: bellagio-vault
spec:
  type: Replica1
  storageClassName: bellagio-vault-class
  pvReclaimPolicy: retain
  storage:
    - node: ${KIND_CLUSTER_NAME}-control-plane
      path: /var/lib/kadalu-ci/bellagio-brick
EOF

kubectl wait -n "${KADALU_NAMESPACE}" --for=create \
  statefulset/kadalu-csi-provisioner --timeout=180s
kubectl wait -n "${KADALU_NAMESPACE}" --for=create \
  daemonset/kadalu-csi-nodeplugin --timeout=180s
kubectl wait -n "${KADALU_NAMESPACE}" --for=create \
  statefulset/server-bellagio-vault-0 --timeout=180s
kubectl rollout status -n "${KADALU_NAMESPACE}" \
  statefulset/kadalu-csi-provisioner --timeout=300s
kubectl wait -n "${KADALU_NAMESPACE}" --for=condition=Ready \
  pod -l app.kubernetes.io/name=kadalu-csi-nodeplugin --timeout=300s
kubectl rollout status -n "${KADALU_NAMESPACE}" \
  statefulset/server-bellagio-vault-0 --timeout=300s
kubectl wait --for=create storageclass/bellagio-vault-class --timeout=120s
test "$(kubectl get storageclass bellagio-vault-class \
  -o jsonpath='{.provisioner}')" = "kadalu"
test "$(kubectl get storageclass bellagio-vault-class \
  -o jsonpath='{.reclaimPolicy}')" = "Retain"
if kubectl get storageclass kadalu.bellagio-vault >/dev/null 2>&1; then
  echo "Unexpected legacy StorageClass was created" >&2
  exit 1
fi
test -z "$(kubectl get storageclass bellagio-vault-class \
  -o jsonpath='{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}')"
test -z "$(kubectl get storageclass bellagio-vault-class \
  -o jsonpath='{.metadata.annotations.storageclass\.beta\.kubernetes\.io/is-default-class}')"

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: bellagio-loot
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: bellagio-vault-class
  resources:
    requests:
      storage: 20Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: danny-ocean-writer
spec:
  containers:
    - name: heist-crew
      image: ${BUSYBOX_IMAGE}
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: loot
          mountPath: /vault
  volumes:
    - name: loot
      persistentVolumeClaim:
        claimName: bellagio-loot
EOF

kubectl wait --for=jsonpath='{.status.phase}'=Bound \
  pvc/bellagio-loot --timeout=300s
kubectl wait --for=condition=Ready pod/danny-ocean-writer --timeout=300s
kubectl exec danny-ocean-writer -- \
  sh -c "printf '%s\\n' 'Heist Movie: Bellagio vault intact' > /vault/loot.txt"
kubectl exec danny-ocean-writer -- \
  grep -Fx "Heist Movie: Bellagio vault intact" /vault/loot.txt

kubectl delete pod danny-ocean-writer --wait=true --timeout=180s
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: rusty-ryan-reader
spec:
  containers:
    - name: heist-crew
      image: ${BUSYBOX_IMAGE}
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: loot
          mountPath: /vault
  volumes:
    - name: loot
      persistentVolumeClaim:
        claimName: bellagio-loot
EOF

kubectl wait --for=condition=Ready pod/rusty-ryan-reader --timeout=300s
kubectl exec rusty-ryan-reader -- \
  grep -Fx "Heist Movie: Bellagio vault intact" /vault/loot.txt

kubectl patch pvc bellagio-loot --type=merge \
  --patch '{"spec":{"resources":{"requests":{"storage":"30Mi"}}}}'
kubectl wait --for=jsonpath='{.status.capacity.storage}'=30Mi \
  pvc/bellagio-loot --timeout=300s
kubectl exec rusty-ryan-reader -- \
  grep -Fx "Heist Movie: Bellagio vault intact" /vault/loot.txt

pv_name="$(kubectl get pvc bellagio-loot -o jsonpath='{.spec.volumeName}')"
kubectl delete pod rusty-ryan-reader --wait=true --timeout=180s
kubectl delete pvc bellagio-loot --wait=true --timeout=180s
kubectl wait --for=jsonpath='{.status.phase}'=Released \
  "persistentvolume/${pv_name}" --timeout=300s
test "$(kubectl get "persistentvolume/${pv_name}" \
  -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')" = "Retain"
kubectl delete "persistentvolume/${pv_name}" --wait=true --timeout=180s

echo "Kubernetes 1.36 provision, mount, remount, expand, and retain passed"
