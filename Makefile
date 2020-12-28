SHELL = /bin/bash
HOST_CACHE := $(shell go env GOCACHE)

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -o output/sqlproxy ./cmd/root.go
clean:
	rm -rf output
docker:
	DOCKER_BUILDKIT=1 docker build -f ./Dockerfile -t sqlproxy .
run: build docker
	docker run -p 3306:3306 sqlproxy
output: build docker
	docker save sqlproxy -o ./sqlproxy.img