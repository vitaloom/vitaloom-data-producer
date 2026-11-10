package main

import (
	"encoding/json"
	mathrand "math/rand"
	"strings"
	"testing"
	"time"
)

func TestHeartRateGeneratorCreatesUniqueMetricPrefixedEvents(t *testing.T) {
	now := time.Date(2026, 10, 24, 2, 0, 0, 0, time.UTC)
	generator := NewEventGenerator("run-test", now.Add(-time.Minute), mathrand.New(mathrand.NewSource(42)))
	seen := make(map[string]bool)
	for i := 0; i < 5; i++ {
		event := generator.nextHeartRateEvent(now.Add(time.Duration(i)*time.Second), generator.activityPhase(now))
		if !strings.HasPrefix(event.EventID, "hr-run-test-") || seen[event.EventID] {
			t.Fatalf("invalid/duplicate event ID %q", event.EventID)
		}
		seen[event.EventID] = true
		if event.HeartRateBPM < 60 || event.HeartRateBPM > 135 || event.OccurredAt.Location() != time.UTC {
			t.Fatalf("out-of-range generated event: %+v", event)
		}
	}
}

func TestHeartRateJSONUsesExactContractFieldNamesAndUTCZ(t *testing.T) {
	at := time.Date(2026, 10, 24, 2, 0, 0, 0, time.UTC)
	batch := authenticatedHeartRateBatch{DeviceID: "device-1", RunID: "run-x", Events: []HeartRateEvent{{EventID: "hr-run-x-000001", OccurredAt: at, HeartRateBPM: 92}}}
	encoded, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, expected := range []string{`"deviceId":"device-1"`, `"runId":"run-x"`, `"eventId":"hr-run-x-000001"`, `"occurredAt":"2026-10-24T02:00:00Z"`, `"heartRateBpm":92`} {
		if !strings.Contains(text, expected) {
			t.Errorf("JSON %s is missing %s", text, expected)
		}
	}
}

func TestNewRunIDIsCompactAndUnique(t *testing.T) {
	now := time.Date(2026, 10, 24, 2, 0, 0, 0, time.UTC)
	first, err := NewRunID(now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRunID(now)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || len(first) > 128 || !strings.HasPrefix(first, "run-20261024T020000Z-") {
		t.Fatalf("unexpected run IDs: first=%q second=%q", first, second)
	}
}
