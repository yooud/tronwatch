.PHONY: build test race vet fmt-check verify

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -o bin/tronwatch ./cmd/tronwatch

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l $$(find cmd internal -name '*.go' -type f))"

verify: fmt-check test vet build
