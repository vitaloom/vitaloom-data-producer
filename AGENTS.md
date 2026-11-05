# Agent instructions

VitaLoom Data Producer is a standalone Go tool for generating synthetic Heart Rate and Steps events and sending them to VitaLoom Backend over HTTP.

## Start here

- [README.md](./README.md) — purpose, current capabilities, setup and commands.
- [Backend API Contract](https://github.com/vitaloom/vitaloom-backend/blob/main/docs/api/api-contract.md) — canonical ingestion paths, payloads, response semantics, event identity and retry behavior.

## Contract and safety rules

- The Backend API Contract is authoritative. Do not invent or independently version request/response shapes in this repository.
- Heart Rate and Steps are separate batch endpoints: `POST /api/heart-rate/batch` and `POST /api/steps/batch`.
- Preserve stable `eventId` values across retries. A retry must send the same logical events; never regenerate IDs or timestamps for an in-flight retry.
- Do not connect to MongoDB or write to backend persistence directly. Publish through the Backend HTTP API.
- Keep simulation time (`occurredAt`) distinct from wall-clock emission time if/when emission timestamps are introduced.
- Generated measurements are synthetic test/demo data, not real wearable readings, clinical measurements or medical advice.
- Load generation must require an explicitly configured, authorized test environment. Keep sensible limits, bounded retries and a clear stop mechanism.
- Do not add Kafka, distributed workers, a separate processing service or dependencies without a measured requirement and explicit agreement.

## Development

Use the Go version declared in `go.mod` (Go 1.23).

From the repository root:

```bash
go test ./...
go vet ./...
go build ./...
```

Run `gofmt` on changed Go files. Report commands actually run and any checks that remain unverified.
