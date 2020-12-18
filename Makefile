SHELL = /bin/bash
HOST_BUILD_CACHE := $(shell go env GOCACHE)

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -o output/sqlproxy ./cmd/root.go
clean:
	rm -rf output
docker:
	docker build --build-arg HOST_BUILD_CACHE=$(HOST_BUILD_CACHE) -f ./Dockerfile -t sqlproxy .
run: build docker
	docker run -p 3306:3306 sqlproxy
buildkit:
	DOCKER_BUILDKIT=1 docker build --build-arg HOST_BUILD_CACHE=$(HOST_BUILD_CACHE) -f ./Dockerfile -t sqlproxy .