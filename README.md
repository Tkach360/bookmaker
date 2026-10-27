# Сервис букмекерской конторы

HTTP-клиент взаимодействует с `gateway`, а внутренние сервисы `gateway`, `bets` и `events` обмениваются сообщениями через RabbitMQ. Контракт межсервисного обмена описан в `asyncapi.yaml`; типы сообщений, схемы и каналы в каталоге `generated/` создаются генератором `go-asyncapi`.

## Возможности HTTP API

Gateway доступен на `http://localhost:8080` при запуске через Docker Compose.

| Метод | Путь | Назначение |
|---|---|---|
| `GET` | `/` | HTML-страница с описанием API |
| `GET` | `/bet/:id` | Получить ставку по идентификатору |
| `POST` | `/bet` | Создать ставку |
| `GET` | `/bet/:id/details` | Получить ставку вместе со связанным событием |
| `GET` | `/event/:id/bets` | Получить ставки, относящиеся к событию |
| `GET` | `/events` | Получить список событий с фильтрацией и пагинацией |

В путях `:id` — положительный числовой идентификатор.

### Получить ставку

```bash
curl http://localhost:8080/bet/1
```

### Создать ставку

```bash
curl -X POST http://localhost:8080/bet \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1,"event_id":1,"amount":"100.50"}'
```

`amount` передаётся строкой, чтобы не терять точность десятичного денежного значения.

### Получить ставку и событие

```bash
curl http://localhost:8080/bet/1/details
```

### Получить ставки на событие

```bash
curl http://localhost:8080/event/1/bets
```

### Получить список событий

```bash
# Все события: limit по умолчанию 20, offset по умолчанию 0
curl 'http://localhost:8080/events'

# Только открытые события
curl 'http://localhost:8080/events?status=open'

# Пагинация
curl 'http://localhost:8080/events?status=win&limit=10&offset=0'
```

## Архитектура

### Сервисы

- **`gateway`** - HTTP API на Gin. Проверяет входные параметры, вызывает клиент межсервисного обмена и формирует HTTP-ответы.
- **`bets`** — логика создания и чтения ставок, получение деталей ставки и выборка ставок по событию. Для работы с информацией о событиях обращается к сервису `events` через RabbitMQ.
- **`events`** - логика событий: получение события и его статуса, создание события, изменение статуса, а также список событий с фильтрацией и пагинацией.
- **RabbitMQ** - транспорт обмена сообщениями между сервисами.

### Поток сообщений

Типичные маршруты сообщений:

- `gateway` отправляет запросы по ставкам в очередь `bets.requests`.
- `gateway` и `bets` отправляют запросы по событиям в `events.requests`.
- Ответы для `gateway` публикуются в `gateway.responses`.
- Ответы на запросы `bets` от `events` публикуются в `bets.responses`.

Запросы и ответы сопоставляются по `correlationId`. Запрос указывает очередь ответа через заголовок `replyTo`.

### AsyncAPI и сгенерированный код

- `asyncapi.yaml` — контракт каналов, сообщений, схем и операций RabbitMQ.
- `generated/channels` — сгенерированные типы каналов и операции запечатывания/распаковки сообщений.
- `generated/messages` — структуры сообщений и их сериализация.
- `generated/schemas` — структуры payload, полученные из схем AsyncAPI.
- `generated/operations` и `generated/servers` — сгенерированные обёртки операций и доступ к серверу.
- `pkg/genamqp` — вспомогательный слой приложения для публикации, маршрутизации по `messageType` и извлечения заголовков. Это не замена сгенерированным сообщениям и каналам.

## Запуск через Docker Compose

Требуются Docker Engine и Docker Compose.

Запустить RabbitMQ и все три сервиса:

```bash
docker compose up --build
```

После запуска:

- HTTP API: `http://localhost:8080`
- RabbitMQ AMQP: `localhost:5672`
- RabbitMQ Management UI: `http://localhost:15672` (в стандартной локальной конфигурации — пользователь `guest`, пароль `guest`)

В Docker Compose сервисы получают адрес брокера через переменную `BROKER_URL`, настроенную на `amqp://guest:guest@rabbitmq:5672/`. При запуске бинарников вне Docker укажите свой адрес RabbitMQ, например через `.env`:

```dotenv
BROKER_URL=amqp://guest:guest@localhost:5672/
```

## Тесты

Unit-тесты сервисов запускаются из соответствующих Go-модулей:

```bash
(cd services/bets && go test ./...)
(cd services/events && go test ./...)
(cd services/gateway && go test ./...)
(cd pkg/store && go test ./...)
```

End-to-end тесты находятся в `tests/e2e/`, используют Docker Compose и требуют работающий Docker:

```bash
cd tests/e2e
go test -v ./...
```

## Технологии

- Go и Go workspace
- Gin для HTTP API
- RabbitMQ для межсервисного обмена
- AsyncAPI 3 для контракта сообщений
- `go-asyncapi` для генерации Go-кода по контракту
- `shopspring/decimal` для точной работы с денежными и десятичными значениями
- Docker Compose для локального запуска сервисов
