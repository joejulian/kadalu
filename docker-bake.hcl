variable "IMAGE_REPOSITORY" {
  default = "kadalu-ci"
}

variable "IMAGE_TAG" {
  default = "test"
}

variable "KADALU_VERSION" {
  default = "devel"
}

variable "IMAGE_BUILDDATE" {
  default = "(unknown)"
}

variable "IMAGE_REVISION" {
  default = "(unknown)"
}

variable "IMAGE_SOURCE" {
  default = "https://github.com/joejulian/kadalu"
}

group "ci-images" {
  targets = ["builder", "csi", "operator", "server", "test-csi"]
}

target "_common" {
  context   = "."
  platforms = ["linux/amd64"]
  output    = ["type=docker"]
  args = {
    builddate = IMAGE_BUILDDATE
    revision  = IMAGE_REVISION
    source    = IMAGE_SOURCE
    version   = KADALU_VERSION
  }
}

target "builder" {
  inherits   = ["_common"]
  dockerfile = "extras/Dockerfile.builder"
  tags       = ["${IMAGE_REPOSITORY}/builder:${IMAGE_TAG}"]
}

target "_product" {
  inherits = ["_common"]
  contexts = {
    kadalu_builder = "target:builder"
  }
  args = {
    builder_image = "kadalu_builder"
  }
}

target "csi" {
  inherits   = ["_product"]
  dockerfile = "csi/Dockerfile"
  target     = "prod"
  tags       = ["${IMAGE_REPOSITORY}/kadalu-csi:${IMAGE_TAG}"]
}

target "operator" {
  inherits   = ["_product"]
  dockerfile = "kadalu_operator/Dockerfile"
  target     = "prod"
  tags       = ["${IMAGE_REPOSITORY}/kadalu-operator:${IMAGE_TAG}"]
}

target "server" {
  inherits   = ["_product"]
  dockerfile = "server/Dockerfile"
  target     = "prod"
  tags       = ["${IMAGE_REPOSITORY}/kadalu-server:${IMAGE_TAG}"]
}

target "test-csi" {
  inherits   = ["_common"]
  dockerfile = "tests/test-csi/Dockerfile"
  target     = "prod"
  tags       = ["${IMAGE_REPOSITORY}/test-csi:${IMAGE_TAG}"]
}
