package main

import (
	"encoding/json"
	mathrand "math/rand"
	"strings"
	"testing"
	"time"
)

func TestNextBatchCreatesUniqueMetricPrefixedEvents(t *testing.T) {
	now := time.Date(2026,10,24,2,0,0,0,time.UTC)
	generator := NewEventGenerator("run-test",now.Add(-time.Minute),mathrand.New(mathrand.NewSource(42)))
	heart, steps := generator.NextBatch(5,now,5*time.Second)
	if heart.RunID != "run-test" || steps.RunID != "run-test" { t.Fatal("run ID not preserved") }
	if len(heart.Events) != 5 || len(steps.Events) != 5 { t.Fatalf("batch sizes = %d and %d",len(heart.Events),len(steps.Events)) }
	seen := make(map[string]bool)
	for i,event := range heart.Events {
		if !strings.HasPrefix(event.EventID,"hr-") { t.Errorf("heart event ID lacks hr- prefix: %q",event.EventID) }
		if seen[event.EventID] { t.Errorf("duplicate event ID: %s",event.EventID) }; seen[event.EventID]=true
		if event.HeartRateBPM < 60 || event.HeartRateBPM > 135 { t.Errorf("heart rate out of simulated bounds: %d",event.HeartRateBPM) }
		if event.OccurredAt.Location()!=time.UTC { t.Errorf("heart event %d is not UTC: %v",i,event.OccurredAt.Location()) }
	}
	for i,event := range steps.Events {
		if !strings.HasPrefix(event.EventID,"steps-") { t.Errorf("step event ID lacks steps- prefix: %q",event.EventID) }
		if seen[event.EventID] { t.Errorf("globally duplicate event ID: %s",event.EventID) }; seen[event.EventID]=true
		if event.StepsDelta < 0 { t.Errorf("negative step increment: %d",event.StepsDelta) }
		if event.OccurredAt.Location()!=time.UTC { t.Errorf("step event %d is not UTC: %v",i,event.OccurredAt.Location()) }
	}
	if !heart.Events[len(heart.Events)-1].OccurredAt.Equal(now) { t.Error("latest heart event should use current batch timestamp") }
	if !steps.Events[len(steps.Events)-1].OccurredAt.Equal(now) { t.Error("latest step event should use current batch timestamp") }
}

func TestJSONUsesExactAPIPayloadFieldNamesAndUTCZ(t *testing.T) {
	at := time.Date(2026,10,24,2,0,0,0,time.UTC)
	payload := HeartRateBatch{RunID:"run-x",Events:[]HeartRateEvent{{EventID:"hr-run-x-000001",OccurredAt:at,HeartRateBPM:92}}}
	encoded,err:=json.Marshal(payload); if err!=nil {t.Fatal(err)}
	text:=string(encoded)
	for _,expected:=range []string{`"runId":"run-x"`,`"eventId":"hr-run-x-000001"`,`"occurredAt":"2026-10-24T02:00:00Z"`,`"heartRateBpm":92`} {
		if !strings.Contains(text,expected) {t.Errorf("JSON %s is missing %s",text,expected)}
	}
}

func TestNewRunIDIsCompactAndUnique(t *testing.T) {
	now:=time.Date(2026,10,24,2,0,0,0,time.UTC)
	first,err:=NewRunID(now); if err!=nil {t.Fatal(err)}
	second,err:=NewRunID(now); if err!=nil {t.Fatal(err)}
	if first==second {t.Fatal("run IDs unexpectedly match")}
	if len(first)>128 {t.Fatalf("run ID too long: %d",len(first))}
	if !strings.HasPrefix(first,"run-20261024T020000Z-") {t.Errorf("unexpected run ID format: %s",first)}
}
