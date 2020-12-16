build:
	CGO_ENABLED=0 go build  -ldflags '-extldflags "-static"' -o output/sqlproxy ./cmd/root.go
	docker build -f ./Dockerfile -t sqlproxy .
clean:
	rm -rf output
run: build
	docker run -p 3306:3306 sqlproxy