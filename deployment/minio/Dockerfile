# MinIO no longer publishes community images. The `quay.io/minio/minio` and
# `minio/minio` images return unauthorised/not found or release binaries
# (`dl.min.io` returns 410) so, the pinned release from source must be 
# built manually.
#
# Replace the tag version `RELEASE.2025-02-18T16-25-55Z` with the desired MinIO 
# release wherever it appears in this file.
#
# Build command:
# $ docker buildx build --platform linux/amd64 -t copo/copo-minio:RELEASE.2025-02-18T16-25-55Z-cpuv1 --push deployment/minio
#
#
# The `-cpuv1` suffix mirrors the upstream tag: built for `GOAMD64=v1` so it runs
# on older x86-64 CPUs.
#
# References:
# - Refer to https://github.com/minio/minio for the source code and release tags.
# - 'Upgrade MinIO Docker image' section for image version upgrade guidance:
#   https://earlham-institute.atlassian.net/wiki/spaces/COPO1/pages/1774583817/VM+Upgrade+update+Nginx+Docker+image+tag#Upgrade-MinIO-Docker-image%5BhardBreak%5D

FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build

ARG RELEASE=RELEASE.2025-02-18T16-25-55Z
ARG TARGETARCH

ENV CGO_ENABLED=0 \
    GOAMD64=v1 \
    MINIO_RELEASE=RELEASE

RUN apk add --no-cache git ca-certificates

WORKDIR /src
RUN git clone --depth 1 --branch "${RELEASE}" https://github.com/minio/minio.git .

# Same flags as upstream's Makefile, with the version taken from the release
# tag (as RFC3339, like official builds) so `minio --version` reports the real
# release, not DEVELOPMENT.
RUN VERSION=$(echo "${RELEASE#RELEASE.}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z$/T\1:\2:\3Z/') && \
    GOARCH=${TARGETARCH} go build -tags kqueue -trimpath \
      -ldflags "$(go run buildscripts/gen-ldflags.go "${VERSION}")" \
      -o /out/minio .

FROM alpine:3.20

ARG RELEASE=RELEASE.2025-02-18T16-25-55Z

LABEL name="MinIO" \
      version="${RELEASE}" \
      description="MinIO ${RELEASE} built from source for COPO"

RUN apk add --no-cache ca-certificates curl

ENV MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key \
    MINIO_ROOT_USER_FILE=access_key \
    MINIO_ROOT_PASSWORD_FILE=secret_key \
    MINIO_KMS_SECRET_KEY_FILE=kms_master_key \
    MINIO_CONFIG_ENV_FILE=config.env

COPY --from=build /out/minio /usr/bin/minio
COPY --from=build /src/dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
COPY --from=build /src/LICENSE /licenses/LICENSE
COPY --from=build /src/CREDITS /licenses/CREDITS

# Checked here, not in the build stage, where a cross-compiled binary can't run.
RUN /usr/bin/minio --version

EXPOSE 9000
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]
