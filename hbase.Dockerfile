#syntax=docker/dockerfile:1.2

# ---------------------
# Prepare scratch image
# ---------------------

FROM alpine:latest as alpine
RUN apk --no-cache add tzdata zip ca-certificates
WORKDIR /usr/share/zoneinfo
# -0 means no compression. Needed because go's
# tz loader doesn't handle compressed data.
RUN zip -q -r -0 /zoneinfo.zip .

FROM scratch as base

ENV ZONEINFO /zoneinfo.zip
COPY --from=alpine /zoneinfo.zip /
COPY --from=alpine /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
# the tls certificates:
COPY --from=alpine /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/


# --------------
# Build binaries
# --------------

FROM golang:latest as golang
WORKDIR /go/src/app
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn

RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|mirrors.ustc.edu.cn/debian-security|g' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install libkrb5-dev -y && \
    rm -rf /var/lib/apt/lists/*

# install deps first
COPY go.* ./
RUN go mod download
# Use .dockerignore to make sure unrelated changes won't invalidates cache
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build make hbase

# ---------------
# Generate Images
# ---------------

FROM base
COPY --from=golang /go/src/app/output/hbase /
ENTRYPOINT ["/hbase"]
