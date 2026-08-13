variable "cn_network" {
  default     = "dc1"
  description = "Data Ceneter that the job needs to be run in"
}

variable "volname" {
  default     = "sample-pool"
  description = "Volume name for Kadalu CSI which is used for all PVC creations purposes"
}

variable "gluster_hosts" {
  default = "ghost.example.com"

  description = <<-EOS
    - External gluster host where the gluster volume is created, started and quota is set
    - Multiple hosts can be supplied like "host1,host2,host3" (no spaces and trimmed endings)
    - Prefer to supply only one or else need to supply the same wherever interpolation is not supported (ex: in volume.hcl files)
    EOS
}

variable "gluster_volname" {
  default     = "dist"
  description = "Gluster volume name in external cluster"
}

variable "kadalu_version" {
  default     = "devel"
  description = "Kadalu CSI version which is tested against Nomad version mentioned in README.md"
}

variable "mount_identity" {
  default = ""

  description = <<-EOS
    Stable UUID for this storage-pool incarnation. Leave empty to derive it
    from volname, or set the same value in both jobs and rotate it whenever a
    same-named backend is recreated.
    EOS
}

variable "gluster_user" {
  default     = "root"
  description = "Remote user in external gluster cluster who has privileges to run gluster cli"
}

variable "ssh_priv_path" {
  default = "~/.ssh/id_rsa"

  description = <<-EOS
    - Path to SSH private key which is used to connect to external gluster
    - Needed only if gluster native quota capabilities is needed
    - If not needed all corresponding SSH related info should be removed from this Job
    - However it is highly recommended to supply SSH Private key for utilizing on the fly PVC expansion capabilities even with external gluster cluster
    - SSH Key will only be used to perform two ops: set quota and change quota
    - Please refer https://kadalu.io/rfcs/0007-Using-GlusterFS-directory-quota-for-external-gluster-volumes.html for more info
    EOS
}

locals {
  ssh_priv_key = file(pathexpand(var.ssh_priv_path))
  volume_id = uuidv5("dns", "${var.volname}.kadalu.io")
  effective_mount_identity = var.mount_identity != "" ? var.mount_identity : uuidv5("dns", "${var.volname}.mount.kadalu.io")
  mount_config_fingerprint = sha256(jsonencode({
    schema             = 1
    type               = "External"
    volname            = var.volname
    volume_id          = local.volume_id
    single_pv_per_pool = false
    gluster_hosts      = sort(distinct(compact(split(",", var.gluster_hosts))))
    gluster_volname    = var.gluster_volname
    gluster_options    = "log-level=DEBUG"
  }))
}

job "kadalu-csi-controller" {
  datacenters = ["${var.cn_network}"]
  type        = "service"

  group "controller" {
    # The CSI driver serializes same-name creates inside this allocation.
    # Keep one serving controller, matching the Kubernetes deployment.
    count = 1

    task "kadalu-controller" {
      driver = "docker"

      template {
        # This is basically a JSON file which is used to connect to external gluster
        # Make sure it follows JSON convention (No comma ',' for last key pair)
        data = <<-EOS
        {
            "volname": "${var.volname}",
            "volume_id": "${local.volume_id}",
            "type": "External",
            "pvReclaimPolicy": "delete",
            "kadalu_format": "native",
            "gluster_hosts": "${var.gluster_hosts}",
            "gluster_volname": "${var.gluster_volname}",
            "gluster_options": "log-level=DEBUG",
            "mount_identity": "${local.effective_mount_identity}",
            "mount_config_fingerprint": "${local.mount_config_fingerprint}"
        }
        EOS

        destination = "${NOMAD_TASK_DIR}/${var.volname}.info"
        change_mode = "noop"
      }

      template {
        data        = "${uuidv5("dns", "kadalu.io")}"
        destination = "${NOMAD_TASK_DIR}/uid"
        change_mode = "noop"
      }

      template {
        data        = "${local.ssh_priv_key}"
        destination = "${NOMAD_SECRETS_DIR}/ssh-privatekey"
        change_mode = "noop"
        perms       = "600"
      }

      template {
        # No need to supply  'SECRET_XXX' key if not using gluster native quota
        data = <<-EOS
        NODE_ID                          = {{ env "node.unique.name" }}
        CSI_ENDPOINT                     = "unix://csi/csi.sock"
        SECRET_GLUSTERQUOTA_SSH_USERNAME = "${var.gluster_user}"
        KADALU_VERSION                   = "${var.kadalu_version}"
        CSI_ROLE                         = "controller"
        VERBOSE                          = "yes"
        EOS

        destination = "${NOMAD_TASK_DIR}/file.env"
        env         = true
      }

      config {
        image = "ghcr.io/joejulian/kadalu-csi:${var.kadalu_version}"

        # Nomad client config for docker plugin should have privileged set to 'true'
        # refer https://www.nomadproject.io/docs/drivers/docker#privileged
        # Need to access '/dev/fuse' for mounting external gluster volume
        privileged = true

        mount {
          # Analogous to kadalu-info configmap
          type = "bind"

          # Make sure the source paths starts with current dir (basically: "./")
          source = "./${NOMAD_TASK_DIR}/${var.volname}.info"

          target   = "/var/lib/gluster/${var.volname}.info"
          readonly = true
        }

        mount {
          # Extra baggage for now, will be taken care in Kadalu in next release
          type     = "bind"
          source   = "./${NOMAD_TASK_DIR}/uid"
          target   = "/var/lib/gluster/uid"
          readonly = true
        }

        mount {
          # If you are not using gluster native quota comment out this stanza
          type     = "bind"
          source   = "./${NOMAD_SECRETS_DIR}/ssh-privatekey"
          target   = "/etc/secret-volume/ssh-privatekey"
          readonly = true
        }

        mount {
          # Logging
          type     = "tmpfs"
          target   = "/var/log/gluster"
          readonly = false

          tmpfs_options {
            # 1 MB
            size = 1000000 # size in bytes
          }
        }
      }

      csi_plugin {
        id        = "kadalu-csi"
        type      = "controller"
        mount_dir = "/csi"
      }
    }
  }
}
