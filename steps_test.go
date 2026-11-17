package main

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStepGenerationBoundsSkipsZeroAndUsesUTCEventIdentity(t *testing.T) {
	var sequence uint64
	if event, ok := makeStepEvent("run-test", &sequence, 0, time.Now()); ok || event.EventID != "" || sequence != 0 {
		t.Fatalf("zero increment created event=%+v ok=%t sequence=%d", event, ok, sequence)
	}
	for _, delta := range []int{-1, 6} {
		if event, ok := makeStepEvent("run-test", &sequence, delta, time.Now()); ok || event.EventID != "" || sequence != 0 {
			t.Fatalf("out-of-range increment %d created event=%+v", delta, event)
		}
	}
	at := time.Date(2026, 11, 10, 1, 2, 3, 456789000, time.FixedZone("test", 7*60*60))
	generator := NewStepEventGenerator("run-test", rand.New(rand.NewSource(12)))
	first, ok := generator.Next(at)
	if !ok || first.StepsDelta < 1 || first.StepsDelta > 5 || first.EventID != "steps-run-test-000001" {
		t.Fatalf("first event=%+v ok=%t", first, ok)
	}
	if first.OccurredAt == nil || first.OccurredAt.Location() != time.UTC || !first.OccurredAt.Equal(at.Truncate(time.Millisecond)) {
		t.Fatalf("occurredAt=%v; expected UTC millisecond time", first.OccurredAt)
	}
	for i := 0; i < 1000; i++ {
		event, present := generator.Next(at.Add(time.Duration(i+1) * time.Second))
		if present && (event.StepsDelta < 1 || event.StepsDelta > 5) {
			t.Fatalf("generated invalid increment %d", event.StepsDelta)
		}
	}
}

func TestStepsIntervalsUseWholeSecondsAndCloseAtHoChiMinhHourBoundary(t *testing.T) {
	location := hoChiMinhLocation
	start := time.Date(2026, 11, 10, 9, 59, 0, 0, location)
	boundary := time.Date(2026, 11, 10, 10, 0, 0, 0, location)
	afterBoundary := boundary.Add(time.Second)
	if !shouldCloseStepsWindow(start, boundary, time.Hour) {
		t.Fatal("Steps window ending exactly at local hour boundary remained open")
	}
	if shouldCloseStepsWindow(boundary, afterBoundary, time.Hour) {
		t.Fatal("Steps window closed before aggregation duration or local hour boundary")
	}
	generator := NewStepEventGenerator("run-interval", rand.New(rand.NewSource(7)))
	event, ok := generator.NextInterval(start, boundary, 300)
	if !ok || event.EventID != "steps-run-interval-000001" || event.OccurredAt != nil || event.StartAt == nil || event.EndAt == nil || event.StepsDelta != 300 {
		t.Fatalf("invalid interval event: %+v ok=%t", event, ok)
	}
	encoded, err := json.Marshal(event)
	if err != nil || strings.Contains(string(encoded), `"occurredAt"`) || !strings.Contains(string(encoded), `"startAt"`) || !strings.Contains(string(encoded), `"endAt"`) {
		t.Fatalf("interval JSON=%s err=%v", encoded, err)
	}
	if _, ok := generator.NextInterval(start, start.Add(500*time.Millisecond), 1); ok {
		t.Fatal("accepted a fractional-second interval")
	}
	if _, ok := generator.NextInterval(start, start.Add(2*time.Second), 11); ok {
		t.Fatal("accepted a delta above five steps per second")
	}
}

func TestStepsClientUsesExactContractRoutePayloadAndBearer(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/api/steps/batch" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected route: %s %s content-type=%q", r.Method, r.URL.Path, r.Header.Get("Content-Type"))
		}
		assertBearer(t, r, "steps-token")
		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Errorf("decode raw body: %v", err)
		}
		if len(raw) != 3 || raw["accountId"] != nil {
			t.Errorf("unexpected envelope keys: %+v", raw)
		}
		var batch authenticatedStepsBatch
		_ = json.Unmarshal(mustMarshal(t, raw), &batch)
		if batch.DeviceID != "steps-device-0001" || batch.RunID != "run-steps" || len(batch.Events) != 1 || batch.Events[0].StepsDelta != 3 || batch.Events[0].OccurredAt != nil || batch.Events[0].StartAt == nil || batch.Events[0].EndAt == nil {
			t.Errorf("unexpected decoded batch: %+v", batch)
		}
		if batch.Events[0].EndAt.Sub(*batch.Events[0].StartAt) != time.Minute {
			t.Errorf("unexpected interval: %+v", batch.Events[0])
		}
		writeStepsResponse(w, batch, []string{"ACCEPTED"}, 1, 0, 0)
	}))
	defer server.Close()
	client := newStepsClient(Config{APIBaseURL: server.URL, Timeout: time.Second}, nil)
	startAt := time.Date(2026, 11, 10, 1, 0, 0, 0, time.UTC)
	endAt := startAt.Add(time.Minute)
	batch := authenticatedStepsBatch{DeviceID: "steps-device-0001", RunID: "run-steps", Events: []StepEvent{{EventID: "steps-run-0001", StartAt: &startAt, EndAt: &endAt, StepsDelta: 3}}}
	encoded, err := json.Marshal(batch)
	if err != nil || !strings.Contains(string(encoded), `"startAt":"2026-11-10T01:00:00Z"`) || !strings.Contains(string(encoded), `"endAt":"2026-11-10T01:01:00Z"`) || strings.Contains(string(encoded), `"occurredAt"`) {
		t.Fatalf("JSON=%s err=%v", encoded, err)
	}
	if _, _, err := client.sendBatch(context.Background(), "steps-token", batch); err != nil {
		t.Fatalf("sendBatch() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d; want 1", calls.Load())
	}
}

func TestStepsClientRetriesIdenticalBatchAndRejectsInconsistentResponse(t *testing.T) {
	var calls atomic.Int32
	var firstBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertBearer(t, r, "retry-token")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read batch: %v", err)
		}
		call := calls.Add(1)
		if call == 1 {
			firstBody = string(body)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if string(body) != firstBody {
			t.Errorf("retry body changed: first=%s next=%s", firstBody, body)
		}
		var batch authenticatedStepsBatch
		_ = json.Unmarshal(body, &batch)
		if call == 2 {
			writeStepsResponse(w, batch, []string{"ACCEPTED", "DUPLICATE"}, 0, 1, 0)
			return
		}
		writeStepsResponse(w, batch, []string{"ACCEPTED", "DUPLICATE"}, 1, 1, 0)
	}))
	defer server.Close()
	client := newStepsClient(Config{APIBaseURL: server.URL, Timeout: time.Second, MaxRetries: 2}, nil)
	startAt := time.Now().UTC().Truncate(time.Second)
	endAt := startAt.Add(time.Minute)
	batch := authenticatedStepsBatch{DeviceID: "device", RunID: "run", Events: []StepEvent{
		{EventID: "a", StartAt: &startAt, EndAt: &endAt, StepsDelta: 1},
		{EventID: "b", StartAt: &startAt, EndAt: &endAt, StepsDelta: 2},
	}}
	if _, _, err := client.sendBatch(context.Background(), "retry-token", batch); err != nil {
		t.Fatalf("sendBatch() error = %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("attempts=%d; want 3", calls.Load())
	}
}

func TestStepsClientEnforcesBatchLimitAndEmptyEnvelope(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	client := newStepsClient(Config{APIBaseURL: server.URL, Timeout: time.Second}, nil)
	for _, events := range [][]StepEvent{nil, make([]StepEvent, maxBatchSize+1)} {
		if _, _, err := client.sendBatch(context.Background(), "token", authenticatedStepsBatch{RunID: "run", DeviceID: "device", Events: events}); err == nil {
			t.Fatal("invalid batch envelope was accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid batches sent %d requests", calls.Load())
	}
}

func TestValidateStepsResponsePreservesPartialResultsAndChecksIdentity(t *testing.T) {
	batch := authenticatedStepsBatch{RunID: "run", Events: []StepEvent{{EventID: "a"}, {EventID: "b"}}}
	response := authenticatedStepsResponse{RunID: "run", Received: 2, Accepted: 1, Rejected: 1, Results: []stepsItemResult{
		{Index: 0, EventID: "a", Status: "ACCEPTED"},
		{Index: 1, EventID: "b", Status: "REJECTED", Error: &stepsItemError{Code: "INVALID_STEPS_DELTA"}},
	}}
	if err := validateStepsResponse(batch, response); err != nil {
		t.Fatalf("valid partial response rejected: %v", err)
	}
	response.Results[1].Index = 0
	if err := validateStepsResponse(batch, response); err == nil {
		t.Fatal("misordered result was accepted")
	}
}

func writeStepsResponse(w http.ResponseWriter, batch authenticatedStepsBatch, statuses []string, accepted, duplicates, rejected int) {
	results := make([]stepsItemResult, len(batch.Events))
	for i, event := range batch.Events {
		results[i] = stepsItemResult{Index: i, EventID: event.EventID, Status: statuses[i]}
		if statuses[i] == "REJECTED" {
			results[i].Error = &stepsItemError{Code: "INVALID_STEPS_DELTA", Message: "invalid test increment"}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(authenticatedStepsResponse{RunID: batch.RunID, Received: len(batch.Events), Accepted: accepted, Duplicates: duplicates, Rejected: rejected, Results: results})
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
