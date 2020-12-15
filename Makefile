build:
	CGO_ENABLED=0 go build -o output/main ./cmd/main.go
clean:
	rm -rf output