# VitaLoom Data Producer

A standalone Go CLI that generates synthetic Heart Rate and Steps events and publishes them to VitaLoom Backend. It is intended for local demos, integration checks and controlled development workloads — not as a source of real wearable data or medical advice.

## Current capabilities

The initial implementation has been migrated from the end-to-end demo:

- Generates batches of synthetic Heart Rate observations and Step increments.
- Sends them to the matching Backend batch endpoints over HTTP.
- Uses stable event IDs and preserves the same request body during retries.
- Supports a configurable batch size, interval, request timeout, bounded retries and optional run duration.
- Logs per-batch results and a run summary.
- Includes unit tests for configuration, event generation, HTTP behavior and producer lifecycle.

The following are future work, not implemented capabilities: profile-driven behavioral scenarios, UI-controlled simulation, load-test profiles, and richer observability.

## Architecture

```text
VitaLoom Data Producer (Go)
        |
        | HTTP batch requests
        v
VitaLoom Backend (Spring Boot)
        |
        v
     MongoDB
```

The Producer never connects to MongoDB. Backend validation, persistence, deduplication and aggregation remain authoritative.

## API contract

The canonical contract lives in [vitaloom-backend/docs/api/api-contract.md](https://github.com/vitaloom/vitaloom-backend/blob/main/docs/api/api-contract.md).

| Method | Endpoint | Event type |
|---|---|---|
| POST | `/api/heart-rate/batch` | Heart Rate observations |
| POST | `/api/steps/batch` | Step increments |

Do not change payload fields, response handling, event identity or timestamp semantics independently of the Backend contract.

## Prerequisites

- Go 1.23 or newer.
- A running VitaLoom Backend with the corresponding ingestion endpoints implemented.
- Network access from the Producer process to the Backend base URL.

## Run locally

Run from the repository root:

```bash
go run . --api-base-url http://localhost:8080 --batch-size 5 --interval 2s --duration 90s
```

Omit `--duration` or set it to `0s` to run until Ctrl+C.

The current defaults are:

| Option | Default | Description |
|---|---:|---|
| `--api-base-url` | `http://localhost` | Backend/API base URL |
| `--batch-size` | `5` | Events per metric batch (1–500) |
| `--interval` | `5s` | Delay between batch cycles |
| `--duration` | `0s` | Unlimited until stopped |
| `--timeout` | `5s` | HTTP attempt timeout |
| `--max-retries` | `3` | Retries after the initial attempt (0–10) |
| `--retry-delay` | `500ms` | Delay between retries |

Every option can also be configured with its corresponding `VITALOOM_*` environment variable, for example `VITALOOM_API_BASE_URL` and `VITALOOM_BATCH_SIZE`.

## Development checks

```bash
gofmt -w *.go
go test ./...
go vet ./...
go build ./...
```

## Repository layout

```text
.
├── main.go
├── config.go
├── events.go
├── client.go
├── producer.go
├── *_test.go
├── go.mod
├── AGENTS.md
└── README.md
```

## Scope boundaries

This repository implements a development simulator. It does not read Apple Health/HealthKit or connect to a physical wearable. Sleep and Water remain manual-entry metrics owned by the Mobile app and Backend. Keep the first deployment simple: one Producer process and one Spring Boot Backend instance.
