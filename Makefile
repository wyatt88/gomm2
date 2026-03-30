.PHONY: build test clean docker lint bench security

VERSION ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/gomm2 ./cmd/gomm2

test:
	go test -v -race -count=1 ./...

test-cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

bench:
	go test -bench=. -benchmem ./...

lint:
	golangci-lint run ./...

security:
	govulncheck ./...

clean:
	rm -rf bin/ coverage.out coverage.html

docker:
	docker build -t gomm2:$(VERSION) -f deploy/Dockerfile .

docker-push: docker
	docker tag gomm2:$(VERSION) $(REGISTRY)/gomm2:$(VERSION)
	docker push $(REGISTRY)/gomm2:$(VERSION)

fmt:
	gofmt -s -w .

vet:
	go vet ./...

mod-tidy:
	go mod tidy

all: fmt vet lint test build
