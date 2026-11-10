package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStepEventGenerationBoundsZeroAndIdentity(t *testing.T) {
	var sequence uint64
	if event, ok := makeStepEvent("run-test", &sequence, 0, time.Now()); ok || event.EventID != "" || sequence != 0 {
		t.Fatalf("zero increment produced event=%+v ok=%t seq=%d", event, ok, sequence)
	}
	for _, delta := range []int{-1, 6} {
		if event, ok := makeStepEvent("run-test", &sequence, delta, time.Now()); ok || event.EventID != "" || sequence != 0 {
			t.Fatalf("out-of-range increment %d produced event=%+v ok=%t", delta, event, ok)
		}
	}
	firstAt := time.Date(2026, 11, 10, 1, 2, 3, 456789000, time.FixedZone("test", 7*60*60))
	generator := NewStepEventGenerator("run-test", time.Time{}, rand.New(rand.NewSource(12)))
	first, ok := generator.Next(firstAt)
	if !ok || first.StepsDelta < 1 || first.StepsDelta > 5 || first.EventID != "steps-run-test-000001" {
		t.Fatalf("first event=%+v ok=%t; want valid first increment and stable sequence", first, ok)
	}
	if first.OccurredAt.Location() != time.UTC || first.OccurredAt.Nanosecond()%int(time.Millisecond) != 0 || !first.OccurredAt.Equal(firstAt.Truncate(time.Millisecond)) {
		t.Fatalf("occurredAt=%s; want UTC millisecond instant equal to source time", first.OccurredAt)
	}
	for i := 0; i < 1000; i++ {
		event, present := generator.Next(firstAt.Add(time.Duration(i+1) * time.Second))
		if !present {
			continue
		}
		if event.StepsDelta < 1 || event.StepsDelta > 5 {
			t.Fatalf("generated invalid increment %d", event.StepsDelta)
		}
	}
}

func TestStepsJSONShapeAndAuthenticatedRoute(t *testing.T) {
	at := time.Date(2026, 11, 10, 1, 0, 1, 0, time.UTC)
	batch := authenticatedStepsBatch{DeviceID: "steps-device-loadtest-0001", RunID: "run-test", Events: []StepEvent{{EventID: "steps-run-test-000001", OccurredAt: at, StepsDelta: 3}}}
	encoded, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 3 || string(body["deviceId"]) != `"steps-device-loadtest-0001"` || string(body["runId"]) != `"run-test"` {
		t.Fatalf("unexpected batch JSON shape: %s", encoded)
	}
	var events []map[string]json.RawMessage
	if err := json.Unmarshal(body["events"], &events); err != nil || len(events) != 1 || len(events[0]) != 3 {
		t.Fatalf("unexpected event JSON shape: %s (err=%v)", body["events"], err)
	}
	for key, want := range map[string]string{"eventId": `"steps-run-test-000001"`, "occurredAt": `"2026-11-10T01:00:01Z"`, "stepsDelta": "3"} {
		if string(events[0][key]) != want {
			t.Errorf("event field %s=%s; want %s", key, events[0][key], want)
		}
	}
	var routeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routeCalls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/api/steps/batch" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request route/method/content-type = %s %s %q", r.Method, r.URL.Path, r.Header.Get("Content-Type"))
		}
		assertBearer(t, r, "step-access")
		var got authenticatedStepsBatch
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode batch: %v", err)
		}
		writeStepsResponse(w, got, []string{"ACCEPTED"}, 1)
	}))
	defer server.Close()
	client := newStepsClient(Config{APIBaseURL: server.URL, Timeout: time.Second}, log.New(io.Discard, "", 0))
	if _, _, err := client.sendBatch(context.Background(), "step-access", batch); err != nil {
		t.Fatalf("sendBatch() error = %v", err)
	}
	if routeCalls.Load() != 1 {
		t.Fatalf("route calls=%d; want 1", routeCalls.Load())
	}
}

func TestStepsClientRetriesSameBodyAndRejectsInconsistentOutcome(t *testing.T) {
	var calls atomic.Int32
	var firstBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/steps/batch" {
			t.Errorf("path=%s; want Steps batch", r.URL.Path)
		}
		assertBearer(t, r, "retry-token")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		call := calls.Add(1)
		if call == 1 {
			firstBody = string(body)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if string(body) != firstBody {
			t.Errorf("retry body changed: first=%s retry=%s", firstBody, body)
		}
		var batch authenticatedStepsBatch
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if call == 2 {
			writeStepsResponse(w, batch, []string{"ACCEPTED", "DUPLICATE"}, 0)
			return
		}
		writeStepsResponse(w, batch, []string{"ACCEPTED", "DUPLICATE"}, 1)
	}))
	defer server.Close()
	batch := authenticatedStepsBatch{DeviceID: "steps-device-1", RunID: "run-retry", Events: []StepEvent{
		{EventID: "event-1", OccurredAt: time.Now().UTC().Truncate(time.Millisecond), StepsDelta: 1},
		{EventID: "event-2", OccurredAt: time.Now().UTC().Truncate(time.Millisecond), StepsDelta: 2},
	}}
	client := newStepsClient(Config{APIBaseURL: server.URL, Timeout: time.Second, MaxRetries: 2}, log.New(io.Discard, "", 0))
	if _, _, err := client.sendBatch(context.Background(), "retry-token", batch); err != nil {
		t.Fatalf("sendBatch() error = %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("attempts=%d; want 3 including one transient failure and one inconsistent response", calls.Load())
	}
}

func TestStepsClientEnforcesBatchLimit(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	client := newStepsClient(Config{APIBaseURL: server.URL, Timeout: time.Second}, nil)
	events := make([]StepEvent, maxBatchSize+1)
	if _, _, err := client.sendBatch(context.Background(), "token", authenticatedStepsBatch{RunID: "run", DeviceID: "device", Events: events}); err == nil || !strings.Contains(err.Error(), "between 1 and 500") {
		t.Fatalf("oversized batch error=%v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("oversized batch made %d HTTP requests", calls.Load())
	}
}

func TestStepsScheduleSkipsEmptyFlushAndReportsPartialOutcomes(t *testing.T) {
	var logs strings.Builder
	var sends atomic.Int32
	options := stepsCommandOptions{accountIndex: 1, batchSize: 10, batchInterval: 3 * time.Second, duration: 2100 * time.Millisecond}
	seed := int64(0)
	for {
		rng := rand.New(rand.NewSource(seed))
		_ = rng.Intn(5)
		if rng.Intn(6) > 0 {
			break
		}
		seed++
	}
	generator := NewStepEventGenerator("run-partial", time.Now(), rand.New(rand.NewSource(seed)))
	err := runStepsSchedule(context.Background(), options, "run-partial", generator, func(_ context.Context, batch authenticatedStepsBatch) (authenticatedStepsResponse, int, error) {
		sends.Add(1)
		if len(batch.Events) == 0 {
			t.Error("scheduler sent an empty flush")
		}
		return authenticatedStepsResponse{
			RunID: batch.RunID, Received: len(batch.Events), Accepted: 1, Duplicates: 0, Rejected: len(batch.Events) - 1,
			Results: func() []stepsItemResult {
				results := make([]stepsItemResult, len(batch.Events))
				for i, event := range batch.Events {
					status := "REJECTED"
					if i == 0 {
						status = "ACCEPTED"
					}
					results[i] = stepsItemResult{Index: i, EventID: event.EventID, Status: status}
					if status == "REJECTED" {
						results[i].Error = &stepsItemError{Code: "INVALID_STEPS_DELTA"}
					}
				}
				return results
			}(),
		}, http.StatusOK, nil
	}, log.New(&logs, "", 0))
	if err != nil {
		t.Fatalf("runStepsSchedule() error = %v", err)
	}
	if sends.Load() == 0 {
		t.Fatal("scheduler sent no batches")
	}
	if !strings.Contains(logs.String(), "steps_rejected") || !strings.Contains(logs.String(), "error_code=INVALID_STEPS_DELTA") {
		t.Fatalf("rejected outcomes not logged separately: %s", logs.String())
	}
}

func writeStepsResponse(w http.ResponseWriter, batch authenticatedStepsBatch, statuses []string, accepted int) {
	results := make([]stepsItemResult, len(batch.Events))
	duplicates, rejected := 0, 0
	for i, event := range batch.Events {
		status := statuses[i]
		results[i] = stepsItemResult{Index: i, EventID: event.EventID, Status: status}
		switch status {
		case "DUPLICATE":
			duplicates++
		case "REJECTED":
			rejected++
			results[i].Error = &stepsItemError{Code: "INVALID_STEPS_DELTA", Message: "invalid test increment"}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(authenticatedStepsResponse{RunID: batch.RunID, Received: len(batch.Events), Accepted: accepted, Duplicates: duplicates, Rejected: rejected, Results: results})
}

func TestStepsAndCombinedCLIUseAuthenticatedMetricRoutes(t *testing.T) {
	var stepsCalls, heartRateCalls, loginCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/login":
			loginCalls.Add(1)
			var request authRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Email != "loadtest-0001@vitaloom.test" {
				t.Errorf("unexpected login request: %+v err=%v", request, err)
			}
			_ = json.NewEncoder(w).Encode(authResponse{AccessToken: "cli-access", RefreshToken: "cli-refresh", ExpiresInSeconds: 900})
		case "/api/steps/batch":
			stepsCalls.Add(1)
			assertBearer(t, r, "cli-access")
			var batch authenticatedStepsBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Errorf("decode Steps batch: %v", err)
			}
			if batch.DeviceID != "steps-device-loadtest-0001" {
				t.Errorf("device=%s", batch.DeviceID)
			}
			writeStepsResponse(w, batch, repeatStatus("ACCEPTED", len(batch.Events)), len(batch.Events))
		case "/api/heart-rate/batch":
			heartRateCalls.Add(1)
			assertBearer(t, r, "cli-access")
			var batch authenticatedHeartRateBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Errorf("decode Heart Rate batch: %v", err)
			}
			results := make([]heartRateItemResult, len(batch.Events))
			for i, event := range batch.Events {
				results[i] = heartRateItemResult{Index: i, EventID: event.EventID, Status: "ACCEPTED"}
			}
			_ = json.NewEncoder(w).Encode(authenticatedHeartRateResponse{RunID: batch.RunID, Received: len(batch.Events), Accepted: len(batch.Events), Results: results})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	password := generatedTestPassword(t)
	t.Setenv("VITALOOM_API_BASE_URL", server.URL)
	t.Setenv("VITALOOM_TEST_PASSWORD", password)
	t.Setenv("VITALOOM_TIMEOUT", "1s")
	t.Setenv("VITALOOM_MAX_RETRIES", "0")
	t.Setenv("VITALOOM_RETRY_DELAY", "0s")
	if err := run([]string{"step", "--account", "1", "--batch-size", "2", "--batch-interval", "1s", "--duration", "1200ms"}); err != nil {
		t.Fatalf("step CLI run() error = %v", err)
	}
	if stepsCalls.Load() == 0 || heartRateCalls.Load() != 0 {
		t.Fatalf("step route calls steps=%d heart-rate=%d", stepsCalls.Load(), heartRateCalls.Load())
	}
	stepsBefore, heartBefore := stepsCalls.Load(), heartRateCalls.Load()
	if err := run([]string{"--account", "1", "--batch-size", "2", "--interval", "1s", "--duration", "1200ms"}); err != nil {
		t.Fatalf("combined CLI run() error = %v", err)
	}
	if stepsCalls.Load() <= stepsBefore || heartRateCalls.Load() <= heartBefore {
		t.Fatalf("combined route calls did not include both metrics: steps=%d heart-rate=%d", stepsCalls.Load()-stepsBefore, heartRateCalls.Load()-heartBefore)
	}
	if loginCalls.Load() < 3 {
		t.Fatalf("login count=%d; want authenticated session(s) in both modes", loginCalls.Load())
	}
}

func repeatStatus(status string, count int) []string {
	statuses := make([]string, count)
	for i := range statuses {
		statuses[i] = status
	}
	return statuses
}

func TestParseStepsAndCombinedFlags(t *testing.T) {
	password := generatedTestPassword(t)
	env := func(key string) string {
		switch key {
		case "VITALOOM_API_BASE_URL":
			return "http://nginx.test"
		case "VITALOOM_TEST_PASSWORD":
			return password
		default:
			return ""
		}
	}
	steps, _, _, err := parseStepsCommand([]string{"--account", "2", "--batch-size", "500", "--batch-interval", "2s", "--duration", "3s"}, env)
	if err != nil || steps.accountIndex != 2 || steps.batchSize != 500 || steps.batchInterval != 2*time.Second || steps.duration != 3*time.Second {
		t.Fatalf("step options=%+v err=%v", steps, err)
	}
	if _, _, _, err := parseStepsCommand([]string{"--batch-size", "501"}, env); err == nil {
		t.Fatal("Steps batch size 501 was accepted")
	}
	combined, cfg, _, err := parseCombinedCommand([]string{"--account", "2", "--batch-size", "9", "--interval", "3s", "--duration", "8s"}, env)
	if err != nil || combined.accountIndex != 2 || combined.batchSize != 9 || combined.batchInterval != 3*time.Second || combined.duration != 8*time.Second || cfg.APIBaseURL != "http://nginx.test" {
		t.Fatalf("combined options=%+v cfg=%+v err=%v", combined, cfg, err)
	}
}
