# VitaLoom Data Producer

A standalone Go-based engine for generating realistic health events and controlled workloads for VitaLoom.

## Why this repository exists

This project has two related goals:

1. **Behavior-based data simulation** — produce plausible heart-rate and step events for demos and for validating the backend's ingestion, ETL, and daily/weekly statistics.
2. **Controlled load generation** — create repeatable workloads to measure backend throughput, latency, error rate, and resource usage.

The producer is a tool for development and testing, not a source of medical advice or real wearable data.

## Initial architecture

```text
VitaLoom Frontend
    | user actions / simulation controls
    v
VitaLoom Backend (Spring Boot, initially one instance)
    | HTTP ingestion API
    ^ 
    | batched health events
VitaLoom Data Producer (Go, separate process)
```

- Spring Boot remains the system's core backend: authentication, profile, manual sleep/water input, event ingestion, persistence, ETL, and statistics APIs.
- The producer sends events to the backend over HTTP (batching where appropriate). It must not write directly to the backend database.
- Sleep and water intake are manual-entry metrics in the current MVP scope. The producer initially focuses on heart rate and steps.
- Keep the first version small: one producer process and one Spring Boot instance. Do not introduce Kafka, distributed workers, or a separate Go processing service without a measured need.

## Workload modes

### 1. Simulation mode

Simulate wearable-like behavior rather than generating unrelated random values.

- Use a profile and scenario to plan behavior over simulation time.
- Generate correlated signals: activity can increase steps and raise heart rate, followed by recovery.
- Keep simulation time distinct from wall-clock emission time.
- Support reproducible runs (for example, a configurable seed) to make tests and demos repeatable.
- Make missing observations distinct from a measured value of zero.

### 2. Load-test mode

Generate a controlled workload to evaluate the backend independently of the demo UI.

The load profile should eventually support:

- target event rate and/or virtual-user count;
- duration and warm-up/ramp-up;
- constant, ramp-up, spike, and soak patterns;
- batch size, request timeout, and bounded retries;
- safe concurrency limits and an explicit stop mechanism.

Virtual users, events per second, and HTTP requests per second are different measurements and must be reported separately. Load tests should run only against an explicitly configured, authorized test environment.

## Initial event contract (proposal)

Agree on the contract with `vitaloom-backend` before implementation. A starting envelope could be:

```json
{
  "eventId": "stable-unique-event-id",
  "runId": "producer-run-id",
  "type": "HEART_RATE_RECORDED",
  "occurredAt": "2026-01-01T07:30:00Z",
  "emittedAt": "2026-01-01T07:30:02Z",
  "payload": {
    "value": 112,
    "unit": "bpm"
  }
}
```

This is illustrative, not a finalized API contract. Define user/device identity, step-event shape, units, timestamp semantics, validation, batching, duplicate handling, authentication, and error behavior jointly with the backend. Use `occurredAt` for simulation/event time and `emittedAt` for wall-clock send time.

## Proposed implementation roadmap

1. **Agree on contracts** — event envelope, batch ingestion request/response, simulation scenario, load profile, and run lifecycle.
2. **Build a minimal Go CLI** — configuration, seeded event generation, heart-rate/step simulation, HTTP batch publisher, graceful shutdown.
3. **Validate the end-to-end path** — send events to the single Spring Boot instance; verify persistence, deduplication behavior, and daily/weekly aggregates.
4. **Add controlled load tests** — ramp load gradually, collect producer-side throughput/errors/latency, and compare with backend/database metrics.
5. **Add observability** — structured logs and per-run metrics first; consider OpenTelemetry and dashboards when useful.
6. **Evaluate scaling only from evidence** — if processing competes with API traffic or near-real-time stream processing becomes a real requirement, consider a separate Go processing service and/or a broker. Do not build these pre-emptively.

## Success criteria for the first useful version

- Generates plausible, reproducible heart-rate and step events.
- Sends events to the backend using a documented, validated contract.
- Can run and stop a simulation cleanly.
- Can run a bounded load profile and report events generated/sent, request rate, failures, and latency.
- Supports end-to-end verification that generated events reach backend persistence and statistics.

## Non-goals for the initial MVP

- Connecting to real Apple Health / HealthKit APIs or collecting real wearable data.
- Owning business rules, user profiles, the database, ETL, or statistics.
- Implementing a distributed load-testing platform, Kafka, Kubernetes, or a separate real-time processing service before measurements justify them.

## Design principles

- **Simple first, extensible by contract.**
- **Realistic simulation and performance load are separate workloads.**
- **Measure before optimizing or splitting services.**
- **Repeatability and data correctness matter as much as raw throughput.**
- **Protect the target system:** bound load, make the target environment explicit, and provide a stop path.
