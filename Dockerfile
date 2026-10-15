# Единый Dockerfile для всех сервисов.
# Сервис выбирается build-аргументом SERVICE (gateway | bets | events),
# итоговый образ - целью (target): gateway или service.
#
#   docker build --build-arg SERVICE=bets --target service -t bets .

FROM golang:1.27.2-alpine AS builder
WORKDIR /app

# Прокси можно переопределить без правки файла:
#   docker-compose build --build-arg GOPROXY=https://proxy.golang.org,direct
ARG GOPROXY=https://goproxy.io,direct
ENV GOPROXY=${GOPROXY}

COPY . .

ARG SERVICE
RUN test -n "$SERVICE" || (echo "build arg SERVICE is required" >&2; exit 1)

# Скачиваем только модули, нужные этому сервису, с повторами при сбоях сети.
# Кэш модулей сохраняется между сборками и между сервисами.
RUN --mount=type=cache,target=/go/pkg/mod \
    for i in 1 2 3 4 5; do \
      go list -deps ./services/${SERVICE}/cmd/main.go >/dev/null && exit 0; \
      echo "dependency download failed (attempt $i), retrying..." >&2; \
      sleep 5; \
    done; \
    exit 1

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /service ./services/${SERVICE}/cmd/main.go

# gateway: шаблоны и порт
FROM alpine:latest AS gateway
WORKDIR /app
COPY --from=builder /service /app/service
COPY services/gateway/templates /app/templates
EXPOSE 8080
ENTRYPOINT ["/app/service"]

# bets, events и любые другие сервисы без дополнительных файлов
# (последняя стадия: собирается, если target не указан)
FROM alpine:latest AS service
WORKDIR /
COPY --from=builder /service /service
ENTRYPOINT ["/service"]
