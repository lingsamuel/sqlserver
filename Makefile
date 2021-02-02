SHELL = /bin/bash
HOST_CACHE := $(shell go env GOCACHE)
IMAGE_TAG ?= lingsamuel/sqlproxy
HBASE_TAG ?= gohbase

hbase:
	go build -tags kerberos -o output/hbase ./cmd/hbase/hbase.go
hbase-docker:
	DOCKER_BUILDKIT=1 docker build -f ./hbase.Dockerfile -t $(HBASE_TAG) .
hbase-bin: hbase-docker
	HBASE_TAG=$(HBASE_TAG) ./hack/hbase-from-image.sh

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -o output/sqlproxy ./cmd/root.go
clean:
	rm -rf output
docker:
	DOCKER_BUILDKIT=1 docker build -f ./Dockerfile -t $(IMAGE_TAG) .
run: build docker
	docker run --rm -p 3306:3306 $(IMAGE_TAG)

push: build docker
	docker push $(IMAGE_TAG)
output: build docker
	docker save $(IMAGE_TAG) -o ./sqlproxy.img
