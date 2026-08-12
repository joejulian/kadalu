ARG builder_image="ghcr.io/joejulian/kadalu-builder:devel"
ARG python_image="python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2"

FROM ${builder_image} AS builder

ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"

COPY requirements/operator-requirements.txt /tmp/

RUN python3 -m pip install -r /tmp/operator-requirements.txt --no-cache-dir && \
    grep -Po '^[\w\.-]*(?=)' /tmp/operator-requirements.txt | xargs -I pkg python3 -m pip show pkg | grep -P '^(Name|Version|Location)'

FROM ${python_image} AS prod

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -yq && \
    apt-get install -y --no-install-recommends procps && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/kadalu/bin:$PATH"

# kubectl binary
COPY --from=builder /usr/bin/kubectl /usr/bin/kubectl

# actual application to be copied here
# using already installed packages from builder for faster build time
COPY --from=builder /kadalu /kadalu

# Keep the copied virtual environment linked to this image's Python runtime.
RUN ln -sfn /usr/local/bin/python3 /kadalu/bin/python3

RUN mkdir -p /kadalu/manifests

COPY templates/services.yaml.j2             /kadalu/templates/services.yaml.j2
COPY templates/server.yaml.j2               /kadalu/templates/server.yaml.j2
COPY templates/csi.yaml.j2                  /kadalu/templates/csi.yaml.j2
COPY templates/csi-driver-object-v1.yaml.j2    /kadalu/templates/csi-driver-object-v1.yaml.j2
COPY templates/configmap.yaml.j2               /kadalu/templates/configmap.yaml.j2
COPY templates/storageclass-kadalu.custom.yaml.j2    /kadalu/templates/storageclass-kadalu.custom.yaml.j2
COPY templates/external-storageclass.yaml.j2         /kadalu/templates/external-storageclass.yaml.j2
COPY lib/kadalulib.py                       /kadalu/kadalulib.py
COPY cli/kubectl_kadalu/utils.py            /kadalu/utils.py
COPY kadalu_operator/main.py                       /kadalu/
COPY kadalu_operator/start.py                      /kadalu/
COPY kadalu_operator/metrics.py                    /kadalu/
COPY kadalu_operator/exporter.py                   /kadalu/
COPY cli/build/kubectl-kadalu               /usr/bin/kubectl-kadalu
COPY lib/startup.sh                         /kadalu/startup.sh

RUN chmod +x /kadalu/startup.sh

RUN groupadd --gid 1000 kadalu && \
    useradd --uid 1000 --gid kadalu --home-dir /kadalu \
        --no-create-home --shell /usr/sbin/nologin kadalu

ARG version="(unknown)"
# Container build time (date -u '+%Y-%m-%dT%H:%M:%S.%NZ')
ARG builddate="(unknown)"
ARG revision="(unknown)"
ARG source="https://github.com/joejulian/kadalu"

LABEL build-date="${builddate}"
LABEL io.k8s.description="KaDalu Operator"
LABEL name="kadalu-operator"
LABEL Summary="KaDalu Operator"
LABEL vcs-type="git"
LABEL vcs-url="https://github.com/joejulian/kadalu"
LABEL vendor="kadalu"
LABEL version="${version}"
LABEL org.opencontainers.image.revision="${revision}"
LABEL org.opencontainers.image.source="${source}"

USER 1000:1000

ENTRYPOINT ["python3", "/kadalu/start.py"]

# Debugging, Comment the above line and
# uncomment below line
# ENTRYPOINT ["tail", "-f", "/dev/null"]
