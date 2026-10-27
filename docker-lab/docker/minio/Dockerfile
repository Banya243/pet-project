# syntax=docker/dockerfile:1
FROM golang:1.24.2-alpine AS builder

RUN apk add --no-cache curl
WORKDIR /src
RUN curl --fail --location --retry 3 \
      https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-04-22T22-12-26Z \
      | tar -xz --strip-components=1
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -p 2 -trimpath \
      -ldflags="-s -w -X github.com/minio/minio/cmd.Version=2025-04-22T22:12:26Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2025-04-22T22-12-26Z -X github.com/minio/minio/cmd.CopyrightYear=2025" \
      -o /out/minio .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /out/minio /usr/local/bin/minio
COPY --from=builder /src/LICENSE /usr/share/licenses/minio/LICENSE
LABEL org.opencontainers.image.source="https://github.com/minio/minio/tree/RELEASE.2025-04-22T22-12-26Z"
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
