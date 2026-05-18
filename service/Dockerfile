FROM golang:1.25-alpine AS base
WORKDIR /pps
FROM base AS builder
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -extldflags '-static' -buildid=" \
    -trimpath \
    -o /main .

FROM base AS dev
COPY service/go.mod service/go.sum ./
RUN go mod download
RUN go install github.com/air-verse/air@latest
COPY service/ .
ENTRYPOINT ["air", "-c", "service/.air.toml"]

FROM alpine:3.20 AS prod
WORKDIR /pps
RUN apk add --no-cache ca-certificates
COPY --from=builder /main /main
COPY service/index.bin.gz service/index.bin.gz
COPY service/normalization.json service/normalization.json
COPY service/mcc_risk.json service/mcc_risk.json
CMD ["/main"]
