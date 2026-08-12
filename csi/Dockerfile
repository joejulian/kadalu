ARG builder_image="joejulian/builder:latest"
ARG python_image="python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2"

FROM ${builder_image} AS builder

ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"

COPY requirements/csi-requirements.txt /tmp/

RUN python3 -m pip install $(grep -vE '^(grpcio|\s*#)' /tmp/csi-requirements.txt) --no-cache-dir && \
    python3 -m pip download --only-binary :all: --dest /tmp/ --no-cache $(grep -oE '^grpcio==.*$' /tmp/csi-requirements.txt) && \
    python3 -m pip install /tmp/*.whl --force-reinstall && \
    grep -Po '^[\w\.-]*(?=)' /tmp/csi-requirements.txt | xargs -I pkg python3 -m pip show pkg | grep -P '^(Name|Version|Location)'

FROM ${python_image} AS prod
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -yq && \
    apt-get install -y --no-install-recommends \
        attr \
        bash \
        inotify-tools \
        libacl1 \
        libgoogle-perftools4 \
        libssl3 \
        libtirpc3 \
        liburcu8 \
        libuuid1 \
        libxml2 \
        logrotate \
        psmisc \
        sqlite3 \
        ssh \
        tini \
        xfsprogs \
        zlib1g && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"

# copy glusterfs installed in /opt
COPY --from=builder /opt /opt

# actual application to be copied here
# using already installed packages from builder for faster build time
COPY --from=builder /kadalu /kadalu

# Keep the copied virtual environment linked to this image's Python runtime.
RUN ln -sfn /usr/local/bin/python3 /kadalu/bin/python3

RUN mkdir -p /kadalu/volfiles /kadalu/templates
RUN mkdir -p /var/log/glusterfs /var/run/gluster

COPY lib/kadalulib.py          /kadalu/
COPY csi/controllerserver.py   /kadalu/
COPY csi/csi_pb2_grpc.py       /kadalu/
COPY csi/csi_pb2.py            /kadalu/
COPY csi/identityserver.py     /kadalu/
COPY csi/main.py               /kadalu/
COPY csi/start.py              /kadalu/
COPY csi/exporter.py           /kadalu/
COPY csi/nodeserver.py         /kadalu/
COPY csi/volumeutils.py        /kadalu/
COPY lib/startup.sh            /kadalu/
COPY csi/quota-crawler.sh      /kadalu/
COPY csi/watch-vol-changes.sh  /kadalu/
COPY csi/client_heal.sh        /kadalu/
COPY csi/remove_archived_pv.py /kadalu/
COPY csi/watch-logrotate.sh    /kadalu/

COPY extras/kadalu-logrotate.conf     /kadalu/logrotate.conf

RUN chmod +x /kadalu/startup.sh
RUN chmod +x /kadalu/client_heal.sh

ARG version="(unknown)"
# Container build time (date -u '+%Y-%m-%dT%H:%M:%S.%NZ')
ARG builddate="(unknown)"
ARG revision="(unknown)"
ARG source="https://github.com/joejulian/kadalu"

LABEL build-date="${builddate}"
LABEL io.k8s.description="KaDalu CSI driver"
LABEL name="kadalu-csi"
LABEL Summary="KaDalu CSI driver"
LABEL vcs-type="git"
LABEL vcs-url="https://github.com/joejulian/kadalu"
LABEL vendor="kadalu"
LABEL version="${version}"
LABEL org.opencontainers.image.revision="${revision}"
LABEL org.opencontainers.image.source="${source}"

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3", "/kadalu/start.py"]

# Debugging, Comment the above line and
# uncomment below line
# ENTRYPOINT ["tail", "-f", "/dev/null"]
