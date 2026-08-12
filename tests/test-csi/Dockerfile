FROM docker.io/library/golang:1.25.12-alpine3.23@sha256:cc985ef6f9c3bf9ece7488129c9abe0a150388ccdfa428d886fc709dca0b230a AS build

ENV CGO_ENABLED=0
ARG CSI_TEST_COMMIT="2f1e2f2aa8b77199c63e926628fc4c377bfa525d"
ARG CSI_TEST_SHA256="b656abfc01662882bbd117ca7ee6b0199ba7c863f2d764851441b7eaf6f3fdd5"
ARG CSI_TEST_VERSION="v5.5.0"

RUN apk add --no-cache curl && \
    curl -fsSL "https://github.com/kubernetes-csi/csi-test/archive/${CSI_TEST_COMMIT}.tar.gz" -o /tmp/csi-test.tar.gz && \
    echo "${CSI_TEST_SHA256}  /tmp/csi-test.tar.gz" | sha256sum -c - && \
    mkdir /src && \
    tar -xzf /tmp/csi-test.tar.gz --strip-components=1 -C /src && \
    cd /src && \
    go build -trimpath \
      -ldflags "-s -w -buildid= -X main.VERSION=${CSI_TEST_VERSION}" \
      -o /opt/csi-sanity ./cmd/csi-sanity

FROM docker.io/library/busybox:1.37.0@sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0 AS prod

# Version supplied at build time
ARG version="(unknown)"
# Container build time (date -u '+%Y-%m-%dT%H:%M:%S.%NZ')
ARG builddate="(unknown)"
ARG revision="(unknown)"
ARG source="https://github.com/joejulian/kadalu"

LABEL build-date="${builddate}"
LABEL org.opencontainers.image.revision="${revision}"
LABEL org.opencontainers.image.source="${source}"
LABEL io.k8s.description="CSI Sanity container for testing KaDalu driver"
LABEL name="kadalu-csi-sanity"
LABEL Summary="KaDalu Driver CSI Sanity"
LABEL vcs-type="git"
LABEL vcs-url="${source}"
LABEL vendor="kadalu"
LABEL version="${version}"

COPY --from=build /opt/csi-sanity /usr/local/sbin/csi-sanity
CMD ["sh"]
