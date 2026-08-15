ARG python_image="python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2"

FROM ${python_image}

ARG GLUSTERFS_REPOSITORY="https://github.com/joejulian/glusterfs.git"
ARG GLUSTERFS_COMMIT="7ac97e7d03a0aa104f36602a158bd6502a182d11"
ARG GLUSTERFS_VERSION="v11.2-5-g7ac97e7d"
ARG KUBECTL_VERSION="v1.36.3"
ARG KUBECTL_SHA256_AMD64="ebbd080e7c2e275093b55915722043257eb24004363e20acb3c4d71919f88336"
ARG KUBECTL_SHA256_ARM64="3d86f24401c41ae5a46ac50eef8865fe891d3647d324a0836f6c63757a126e62"
ARG SOURCE_DATE_EPOCH="1786760070"
ARG TARGETARCH
ARG revision="(unknown)"
ARG source="https://github.com/joejulian/kadalu"

ENV DEBIAN_FRONTEND=noninteractive
ENV GRPC_PYTHON_BUILD_EXT_COMPILER_JOBS=8
ENV PATH="/kadalu/bin:/opt/sbin:/opt/bin:$PATH"
ENV SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}"
ENV VIRTUAL_ENV=/kadalu

RUN apt-get update -yq && \
    apt-get install -y --no-install-recommends \
        autoconf \
        automake \
        bison \
        build-essential \
        ca-certificates \
        curl \
        e2fsprogs \
        flex \
        g++ \
        git \
        google-perftools \
        libffi-dev \
        libfuse-dev \
        libacl1-dev \
        libgoogle-perftools-dev \
        libgoogle-perftools4 \
        libssl-dev \
        libtcmalloc-minimal4 \
        libtirpc-dev \
        libtool \
        liburcu-dev \
        liburcu8 \
        libuuid1 \
        libxml2-dev \
        net-tools \
        openssl \
        pkg-config \
        sqlite3 \
        telnet \
        uuid-dev \
        wget \
        xfsprogs \
        zlib1g-dev && \
    git init /usr/src/glusterfs && \
    git -C /usr/src/glusterfs remote add origin "${GLUSTERFS_REPOSITORY}" && \
    git -C /usr/src/glusterfs fetch --depth=1 origin "${GLUSTERFS_COMMIT}" && \
    git -C /usr/src/glusterfs checkout --detach FETCH_HEAD && \
    test "$(git -C /usr/src/glusterfs rev-parse HEAD)" = "${GLUSTERFS_COMMIT}" && \
    printf '%s\n' "${GLUSTERFS_VERSION}" > /usr/src/glusterfs/VERSION && \
    cd /usr/src/glusterfs && \
    ./autogen.sh && \
    am_cv_python_version=3.12 PYTHON=/usr/local/bin/python3 ./configure \
        --prefix=/opt \
        --disable-linux-io_uring \
        --disable-lto && \
    make -j"$(nproc)" && \
    make install && \
    ldd /opt/lib/libglusterfs.so.0 | grep -q libtcmalloc && \
    test -f /opt/lib/python3.12/site-packages/gluster/cliutils/cliutils.py && \
    case "${TARGETARCH}" in \
        amd64) kubectl_sha256="${KUBECTL_SHA256_AMD64}" ;; \
        arm64) kubectl_sha256="${KUBECTL_SHA256_ARM64}" ;; \
        *) echo "Unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    curl --fail --location --proto '=https' --tlsv1.2 \
        "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
        --output /tmp/kubectl && \
    printf '%s  %s\n' "${kubectl_sha256}" /tmp/kubectl | sha256sum --check --strict && \
    install -m 0755 /tmp/kubectl /usr/bin/kubectl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /usr/src/glusterfs /tmp/kubectl

COPY requirements/builder-requirements.txt /tmp/
RUN python3 -m venv "${VIRTUAL_ENV}" && \
    "${VIRTUAL_ENV}/bin/pip" install \
        --disable-pip-version-check \
        --no-cache-dir \
        --requirement /tmp/builder-requirements.txt && \
    grep -Po '^[\w\.-]*(?=)' /tmp/builder-requirements.txt | \
        xargs -I pkg python3 -m pip show pkg | \
        grep -P '^(Name|Version|Location)'

RUN sed -i \
    's/include-system-site-packages = false/include-system-site-packages = true/g' \
    /kadalu/pyvenv.cfg

LABEL org.opencontainers.image.revision="${revision}"
LABEL org.opencontainers.image.source="${source}"

# Debugging, comment the line above and uncomment the line below.
ENTRYPOINT ["tail", "-f", "/dev/null"]
