#!/bin/bash

MOUNT_DIR=/mnt

echo "Starting the quota crawler script"
count=0
while true; do

  dirs=$(find $MOUNT_DIR/*/subvol -mindepth 1 -maxdepth 1 -type d -printf '.' 2>/dev/null | wc -c)
  if [ "$dirs" -lt 1 ]; then
    if [ $((count % 100)) -eq 0 ]; then
      echo "No PVC yet, continuing to watch..."
    fi
    sleep 10
    count=$((count + 1))
    continue
  fi

  # Subdir is in the form /mnt/$host-volname/subvol/NN/MM/PVCNAME
  # PVC paths are Kubernetes-generated and cannot contain shell whitespace.
  # shellcheck disable=SC2044
  for dir in $(find $MOUNT_DIR/*/subvol/*/* -maxdepth 1 -mindepth 1 -type d); do
    used_size=$(df -B1 "${dir}" | tail -n1 | awk '{print $3}')
    out=""
    if ! out=$(LC_ALL=C setfattr -n glusterfs.quota.total-usage \
      -v "${used_size}" "${dir}" 2>&1); then
      if [[ "$out" =~ "Operation not supported" ]]; then
        # This is expected when the mounted volume does not support quotas.
        out=""
      elif [[ "$out" =~ "Operation not permitted" ]]; then
        echo "Failed to update quota usage on ${dir}: ${out}" >&2

        # A server restart or self-heal can leave the namespace marker
        # missing. Only EPERM identifies that recoverable state; other
        # failures must not cause a protected namespace xattr write.
        namespace_out=""
        if namespace_out=$(LC_ALL=C setfattr \
          -n trusted.glusterfs.namespace -v "true" "${dir}" 2>&1); then
          if ! out=$(LC_ALL=C setfattr -n glusterfs.quota.total-usage \
            -v "${used_size}" "${dir}" 2>&1); then
            if [[ "$out" =~ "Operation not supported" ]]; then
              out=""
            else
              echo "Failed to update quota usage on ${dir} after restoring namespace: ${out}" >&2
            fi
          fi
        else
          echo "Failed to restore quota namespace on ${dir}: ${namespace_out}" >&2
        fi
      else
        echo "Failed to update quota usage on ${dir}: ${out}" >&2
      fi
    fi
    if [ $((count % 1000)) -eq 0 ]; then
      echo "Latest consumption on $dir : $used_size"
      echo "Empty if setfattr is successful: --$out--"
    fi
  done

  sleep 5
  count=$((count + 1))
done

echo "Exiting the quota crawler script"
