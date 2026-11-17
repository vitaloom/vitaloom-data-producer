# VitaLoom Data Producer

A standalone Go CLI that generates synthetic Heart Rate and Steps events and publishes them through the configured VitaLoom Nginx ingress. It is intended for local demos, integration checks and controlled development workloads — not as a source of real wearable data or medical advice.

## Current capabilities

The initial implementation has been migrated from the end-to-end demo:

- Generates synthetic Heart Rate observations and aggregated Steps intervals.
- Sends them to the matching Backend batch endpoints over HTTP.
- Uses stable event IDs and preserves the same request body during retries.
- Supports independent Heart Rate sampling, Steps aggregation and request sync schedules, a batch cap, bounded retries and optional run duration.
- Logs per-batch results and a run summary.
- Includes unit tests for configuration, event generation, HTTP behavior and producer lifecycle.
- Provisions deterministic test accounts through Nginx and keeps authentication sessions in memory for callers.

The following are future work, not implemented capabilities: profile-driven behavioral scenarios, UI-controlled simulation, load-test profiles, and richer observability.

## Automated tests

Run this from the repository root. The tests use a local fake API.

```powershell
go test ./...
```

## Architecture

```text
VitaLoom Data Producer (Go)
        |
        | HTTP API requests
        v
   Nginx ingress
        |
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
- A running VitaLoom test environment with Nginx routing to the corresponding Backend endpoints.
- An explicitly authorized test Nginx URL reachable from the Producer process.

## Run locally

From the repository root, copy `.env.example` to `.env`, then set `VITALOOM_API_BASE_URL` to your authorized test Nginx URL and `VITALOOM_TEST_PASSWORD` to a password of at least eight characters. The CLI reads `.env` from its current directory; process environment variables take precedence. Values use simple `NAME=value` lines, without quoting or expansion. Provision accounts `1`–`1000` as needed; existing accounts keep their current password.

```powershell
Copy-Item .env.example .env
```

After editing `.env`, provision the accounts you need (`go run . provision` creates all 1,000):

```powershell
go run . provision --from 1 --to 10
```

Without a selector, the Producer runs both metrics. Use `step` or `heart-rate` to run one metric. Heart Rate sampling defaults to once every 300 seconds. Steps are simulated internally once per second, aggregated into 60-second intervals, and sent on a separate 60-second sync schedule. Every run uses authenticated scheduled ingestion. The multi-user example requires ten provisioned accounts.

```powershell
go run . --account 1 --duration 30s
go run . step --account 1 --duration 30s
go run . heart-rate --account 1 --heart-rate-interval 5s --sync-interval 5s --duration 30s
go run . --virtual-users 10 --ramp-up 5s --duration 70s --sync-interval 10s
```

Flags take precedence over environment values, which take precedence over defaults. `--sync-interval` takes precedence over the compatibility aliases `--batch-interval` and `--interval`; at the environment level `VITALOOM_SYNC_INTERVAL` takes precedence over `VITALOOM_BATCH_INTERVAL`, then `VITALOOM_INTERVAL`.

| Option | Default | Description |
|---|---:|---|
| `--api-base-url` | required | Authorized test Nginx URL; can also use `VITALOOM_API_BASE_URL` |
| `--account` | `1` | Provisioned account index (1–1000); mutually exclusive with `--virtual-users` |
| `--virtual-users` | unset | Run accounts 1 through N (1–1000); can also use `VITALOOM_VIRTUAL_USERS` |
| `--devices-per-user` | `1` | Only one simulated device per account is supported |
| `--ramp-up` | `0s` | Stagger VU starts; can also use `VITALOOM_RAMP_UP` |
| `--batch-size` | `5` | Maximum events in each metric request (1–500) |
| `--heart-rate-interval` | `300s` | Heart Rate sample cadence; can use `VITALOOM_HEART_RATE_INTERVAL` |
| `--steps-aggregation-interval` | `60s` | Steps aggregation window in whole seconds (1s–1h); can use `VITALOOM_STEPS_AGGREGATION_INTERVAL` |
| `--sync-interval` | `60s` | Request sync cadence for queued batches; can use `VITALOOM_SYNC_INTERVAL` |
| `--batch-interval` | alias | Compatibility alias for `--sync-interval`; environment fallback `VITALOOM_BATCH_INTERVAL` |
| `--interval` | alias | Legacy compatibility alias for `--sync-interval`; environment fallback `VITALOOM_INTERVAL` |
| `--duration` | `0s` | Run until stopped when zero; can also use `VITALOOM_DURATION` |
| `--timeout` | `5s` | Per-request timeout; can also use `VITALOOM_TIMEOUT` |
| `--max-retries` | `3` | Retries after the initial request (0–10); can also use `VITALOOM_MAX_RETRIES` |
| `--retry-delay` | `500ms` | Delay between retry attempts; can also use `VITALOOM_RETRY_DELAY` |

Heart Rate and Steps have independent schedules. No immediate sample is emitted at startup: the first Heart Rate observation follows its configured interval, and Steps begin after the first internal one-second simulation tick. During normal operation, a Steps interval is queued only after its aggregation window closes; sync phases are spread evenly across VUs over one sync period. On shutdown, queued events are drained and any positive whole-second Steps prefix ending at the last observed tick is finalized and sent. No timestamp is extended into the future. Zero-step aggregates are omitted, and a run shorter than one tick sends no Steps event. Consequently, a short run may contain only a partial Steps interval and no Heart Rate observation. `VITALOOM_TEST_PASSWORD` is required for every run. `provision` remains a separate account setup command.

Each virtual-user worker stops independently if its requests exhaust their bounded retries. Other workers continue until the configured duration or an external stop; after all workers finish, the command exits with an error if any worker failed. Worker lifecycle logs include UTC timestamps, a safe stop cause, the time generation stopped, and the time draining completed.

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
