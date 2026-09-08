# Control plane image.
#
# Multi-stage, ending on a distroless base: the final image has no shell, no
# package manager and no busybox. That is not decoration — the control plane
# mounts the Docker socket, so anything that can execute in this container is
# one step from root on the host. Removing the tools an attacker would reach for
# is cheap and worth doing.

# --- build ---
FROM golang:1.27-alpine AS build

WORKDIR /src

# Copy the module files first so dependency download is cached independently of
# source changes. Without this, every edit re-downloads the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
# CGO_ENABLED=0 produces a static binary, which is what lets the final stage be
# distroless static rather than a full libc image.
RUN CGO_ENABLED=0 go build \
        -trimpath \
        -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/ephemerad ./cmd/ephemerad \
    && CGO_ENABLED=0 go build \
        -trimpath \
        -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/ephemera ./cmd/ephemera

# An empty, correctly-owned /data to seed the runtime image with. Built here
# because the distroless runtime has no shell to mkdir with.
RUN mkdir -p /data

# --- runtime ---
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ephemerad /usr/local/bin/ephemerad
COPY --from=build /out/ephemera /usr/local/bin/ephemera

# nonroot (uid 65532) comes from the base image. The control plane needs no
# privileges of its own; the Docker socket it is given is the whole of its
# authority, and that is deliberately visible in the compose file rather than
# hidden behind a root user here.
USER nonroot:nonroot

# The data directory holds the queue, artifacts and process-driver
# environments. Mount a volume here or everything is lost on restart.
#
# It must exist *in the image*, owned by the runtime user, before VOLUME is
# declared. Docker seeds a fresh named volume from the image's contents at that
# path - including ownership - but when the path does not exist it creates the
# mountpoint root-owned instead. This container runs as uid 65532, so that left
# the control plane unable to create its own queue file and crash-looping on
# "permission denied", which is how `docker compose up` came to be broken.
# 65532 is nonroot in the distroless base; numeric because --chown resolves
# names against the *build* stage's passwd, not the runtime image's.
COPY --from=build --chown=65532:65532 /data /data
VOLUME ["/data"]
ENV EPHEMERA_DATA_DIR=/data

EXPOSE 8080

# No HEALTHCHECK: the orchestrator should poll /readyz, which reports whether
# storage is reachable. A container-level healthcheck would duplicate that and
# gives an orchestrator a second, inconsistent opinion about the same question.

ENTRYPOINT ["/usr/local/bin/ephemerad"]
