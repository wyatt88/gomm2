FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git make
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=$(git describe --tags --always 2>/dev/null || echo dev)" -o /gomm2 ./cmd/gomm2

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 10001 -S gomm2 && \
    adduser -u 10001 -S gomm2 -G gomm2
COPY --from=builder /gomm2 /usr/local/bin/gomm2
USER 10001
EXPOSE 9090
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s CMD wget -q -O- http://localhost:9090/healthz || exit 1
ENTRYPOINT ["gomm2"]
CMD ["--config", "/etc/gomm2/config.yaml"]
