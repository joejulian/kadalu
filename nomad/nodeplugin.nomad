# Please refer 'controller.nomad' file for  variable and job descriptions
variable "cn_network" {
  default = "dc1"
}

variable "volname" {
  default = "sample-pool"
}

variable "gluster_hosts" {
  default = "ghost.example.com"
}

variable "gluster_volname" {
  default = "dist"
}

variable "kadalu_version" {
  default = "devel"
}

variable "mount_identity" {
  default = ""

  description = <<-EOS
    Stable UUID for this storage-pool incarnation. Leave empty to derive it
    from volname, or set the same value in both jobs and rotate it whenever a
    same-named backend is recreated.
    EOS
}

locals {
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

job "kadalu-csi-nodeplugin" {
  datacenters = ["${var.cn_network}"]

  # Should be running on every nomad client
  type = "system"

  update {
    stagger      = "5s"
    max_parallel = 1
  }

  group "nodeplugin" {
    task "kadalu-nodeplugin" {
      driver = "docker"

      template {
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
        data = <<-EOS
        NODE_ID        = {{ env "node.unique.name" }}
        CSI_ENDPOINT   = "unix://csi/csi.sock"
        KADALU_VERSION = "${var.kadalu_version}"
        CSI_ROLE       = "nodeplugin"
        CSI_MOUNT_DIR  = "/csi"
        VERBOSE        = "yes"
        EOS

        destination = "${NOMAD_TASK_DIR}/file.env"
        env         = true
      }

      config {
        image = "ghcr.io/joejulian/kadalu-csi:${var.kadalu_version}"

        privileged = true

        mount {
          type     = "bind"
          source   = "./${NOMAD_TASK_DIR}/${var.volname}.info"
          target   = "/var/lib/gluster/${var.volname}.info"
          readonly = true
        }

        mount {
          type     = "bind"
          source   = "./${NOMAD_TASK_DIR}/uid"
          target   = "/var/lib/gluster/uid"
          readonly = true
        }

        mount {
          type     = "tmpfs"
          target   = "/var/log/gluster"
          readonly = false

          tmpfs_options {
            size = 1000000
          }
        }
      }

      csi_plugin {
        id        = "kadalu-csi"
        type      = "node"
        mount_dir = "/csi"
      }
    }
  }
}
