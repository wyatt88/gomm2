FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git make
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=$(git describe --tags --always 2>/dev/null || echo dev)" -o /gomm2 ./cmd/gomm2

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /gomm2 /usr/local/bin/gomm2
ENTRYPOINT ["gomm2"]
CMD ["--config", "/etc/gomm2/config.yaml"]
