FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.work go.work.sum go.mod go.sum ./
COPY pkg/broker/go.mod pkg/broker/go.sum ./pkg/broker/
COPY pkg/rmq/go.mod pkg/rmq/go.sum ./pkg/rmq/
COPY pkg/store/go.mod ./pkg/store/
COPY services/bets/go.mod services/bets/go.sum ./services/bets/
COPY services/events/go.mod services/events/go.sum ./services/events/
COPY services/gateway/go.mod services/gateway/go.sum ./services/gateway/
COPY shared/go.mod shared/go.sum ./shared/

ENV GOPROXY=https://goproxy.io,direct
RUN --mount=type=cache,target=/go/pkg/mod go work sync && go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod go build -o /service ./services/events/cmd/main.go

FROM alpine:latest
WORKDIR /
COPY --from=builder /service /service
ENTRYPOINT ["/service"]
