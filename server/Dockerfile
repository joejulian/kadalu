ARG builder_image="joejulian/builder:latest"
ARG python_image="python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2"

FROM ${builder_image} AS builder

ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"

COPY requirements/server-requirements.txt /tmp/

RUN python3 -m pip install -r /tmp/server-requirements.txt --no-cache-dir && \
    grep -Po '^[\w\.-]*(?=)' /tmp/server-requirements.txt | xargs -I pkg python3 -m pip show pkg | grep -P '^(Name|Version|Location)'

FROM ${python_image} AS prod

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"
COPY --from=builder /opt /opt

# actual application to be copied here
# using already installed packages from builder for faster build time
COPY --from=builder /kadalu /kadalu

# Keep the copied virtual environment linked to this image's Python runtime.
RUN ln -sfn /usr/local/bin/python3 /kadalu/bin/python3

RUN apt-get update -yq && \
    apt-get install -y --no-install-recommends \
        attr \
        ca-certificates \
        curl \
        libacl1 \
        libgoogle-perftools4 \
        libssl3 \
        libtirpc3 \
        liburcu8 \
        libuuid1 \
        libxml2 \
        procps \
        sqlite3 \
        xfsprogs \
        zlib1g && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ARG VOLGEN_VERSION="v0.4.1"
ARG VOLGEN_SHA256_AMD64="9a40725219b9b40829543339084dea2a14b29b17e3e3b67837a27a49d41b107a"
ARG VOLGEN_SHA256_ARM64="7f53686b5d48e6c867a3d139ffea1dd78a85500d9725827ca127eabd028a26d4"
ARG VOLGEN_CLIENT_TEMPLATE_SHA256="0bc8da0a49aaf2738570d903f14f9bf596201211aa3d54e0b60e0dbc55116ea2"
ARG VOLGEN_SHD_TEMPLATE_SHA256="909596a96536b4c72055f91e72a48083ed0e6df1e2c943347cc4bad905a2edd2"
ARG VOLGEN_STORAGE_UNIT_TEMPLATE_SHA256="cddf12288b8980e4d421e8bc444627ef86cb6a6658a58c81622ccb6c6741983b"
ARG VOLGEN_PYTHON_MODULE_SHA256="6c434822177a5e4703a98a0a362a27c8ab1feac8094fcff411cb9803f510fa13"

RUN case "${TARGETARCH}" in \
        amd64) volgen_sha256="${VOLGEN_SHA256_AMD64}" ;; \
        arm64) volgen_sha256="${VOLGEN_SHA256_ARM64}" ;; \
        *) echo "Unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    volgen_url="https://github.com/kadalu/volgen/releases/download/${VOLGEN_VERSION}" && \
    curl --fail --location --proto '=https' --tlsv1.2 \
        "${volgen_url}/kadalu-volgen-${TARGETARCH}" \
        --output /tmp/kadalu-volgen && \
    curl --fail --location --proto '=https' --tlsv1.2 \
        "${volgen_url}/client.vol.j2" --output /tmp/client.vol.j2 && \
    curl --fail --location --proto '=https' --tlsv1.2 \
        "${volgen_url}/shd.vol.j2" --output /tmp/shd.vol.j2 && \
    curl --fail --location --proto '=https' --tlsv1.2 \
        "${volgen_url}/storage_unit.vol.j2" --output /tmp/storage_unit.vol.j2 && \
    curl --fail --location --proto '=https' --tlsv1.2 \
        "${volgen_url}/__init__.py" --output /tmp/volgen-init.py && \
    printf '%s  %s\n' "${volgen_sha256}" /tmp/kadalu-volgen | \
        sha256sum --check --strict && \
    printf '%s  %s\n' "${VOLGEN_CLIENT_TEMPLATE_SHA256}" /tmp/client.vol.j2 | \
        sha256sum --check --strict && \
    printf '%s  %s\n' "${VOLGEN_SHD_TEMPLATE_SHA256}" /tmp/shd.vol.j2 | \
        sha256sum --check --strict && \
    printf '%s  %s\n' "${VOLGEN_STORAGE_UNIT_TEMPLATE_SHA256}" /tmp/storage_unit.vol.j2 | \
        sha256sum --check --strict && \
    printf '%s  %s\n' "${VOLGEN_PYTHON_MODULE_SHA256}" /tmp/volgen-init.py | \
        sha256sum --check --strict && \
    install -m 0755 /tmp/kadalu-volgen /usr/bin/kadalu-volgen && \
    install -D -m 0644 /tmp/client.vol.j2 \
        /var/lib/kadalu/templates/client.vol.j2 && \
    install -D -m 0644 /tmp/shd.vol.j2 \
        /var/lib/kadalu/templates/shd.vol.j2 && \
    install -D -m 0644 /tmp/storage_unit.vol.j2 \
        /var/lib/kadalu/templates/storage_unit.vol.j2 && \
    volgen_python_dir="$(python3 -c \
        'import sysconfig; print(sysconfig.get_paths()["purelib"])')/kadalu_volgen" && \
    install -D -m 0644 /tmp/volgen-init.py "${volgen_python_dir}/__init__.py" && \
    rm -f \
        /tmp/client.vol.j2 \
        /tmp/kadalu-volgen \
        /tmp/shd.vol.j2 \
        /tmp/storage_unit.vol.j2 \
        /tmp/volgen-init.py

RUN mkdir -p /kadalu/templates /kadalu/volfiles
RUN mkdir -p /var/run/gluster /var/log/glusterfs

COPY lib/kadalulib.py        /kadalu/kadalulib.py
COPY server/server.py        /kadalu/server.py
COPY server/glusterfsd.py    /kadalu/glusterfsd.py
COPY server/shd.py           /kadalu/shd.py
COPY server/mount-glustervol /usr/bin/mount-glustervol
COPY lib/startup.sh          /kadalu/startup.sh
COPY server/stop-server.sh   /kadalu/stop-server.sh
COPY server/exporter.py      /kadalu/exporter.py
COPY server/heal-info.sh     /kadalu/heal-info.sh
COPY server/serverutils.py   /kadalu/serverutils.py

COPY server/watch_volfile_changes.py /kadalu/watch_volfile_changes.py

RUN chmod +x /usr/bin/mount-glustervol
RUN chmod +x /kadalu/startup.sh
RUN chmod +x /kadalu/stop-server.sh
RUN chmod +x /kadalu/heal-info.sh

ARG version="(unknown)"
# Container build time (date -u '+%Y-%m-%dT%H:%M:%S.%NZ')
ARG builddate="(unknown)"
ARG revision="(unknown)"
ARG source="https://github.com/joejulian/kadalu"

LABEL build-date="${builddate}"
LABEL io.k8s.description="KaDalu container(glusterfsd or glustershd)"
LABEL name="kadalu-server"
LABEL Summary="KaDalu Server"
LABEL vcs-type="git"
LABEL vcs-url="https://github.com/joejulian/kadalu"
LABEL vendor="kadalu"
LABEL version="${version}"
LABEL org.opencontainers.image.revision="${revision}"
LABEL org.opencontainers.image.source="${source}"

ENTRYPOINT ["python3", "/kadalu/server.py"]

# Debugging, Comment the above line and
# uncomment below line
# ENTRYPOINT ["tail", "-f", "/dev/null"]
