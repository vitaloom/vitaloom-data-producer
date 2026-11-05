package main

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"time"
)

type HeartRateEvent struct {
	EventID string `json:"eventId"`
	OccurredAt time.Time `json:"occurredAt"`
	HeartRateBPM int `json:"heartRateBpm"`
}
type HeartRateBatch struct {
	RunID string `json:"runId"`
	Events []HeartRateEvent `json:"events"`
}
type StepEvent struct {
	EventID string `json:"eventId"`
	OccurredAt time.Time `json:"occurredAt"`
	StepsDelta int `json:"stepsDelta"`
}
type StepBatch struct {
	RunID string `json:"runId"`
	Events []StepEvent `json:"events"`
}

type EventGenerator struct {
	runID string
	startedAt time.Time
	rng *mathrand.Rand
	heartSeq uint64
	stepsSeq uint64
}
func NewEventGenerator(runID string, startedAt time.Time, rng *mathrand.Rand) *EventGenerator {
	if rng == nil { rng = mathrand.New(mathrand.NewSource(time.Now().UnixNano())) }
	return &EventGenerator{runID: runID, startedAt: startedAt.UTC(), rng: rng}
}
func NewRunID(now time.Time) (string, error) {
	var random [6]byte
	if _, err := cryptorand.Read(random[:]); err != nil { return "", fmt.Errorf("generate run ID: %w", err) }
	return fmt.Sprintf("run-%s-%s", now.UTC().Format("20060102T150405Z"), hex.EncodeToString(random[:])), nil
}
func (g *EventGenerator) NextBatch(batchSize int, now time.Time, interval time.Duration) (HeartRateBatch, StepBatch) {
	heartBatch := HeartRateBatch{RunID: g.runID, Events: make([]HeartRateEvent, 0, batchSize)}
	stepBatch := StepBatch{RunID: g.runID, Events: make([]StepEvent, 0, batchSize)}
	spacing := interval / time.Duration(batchSize)
	if spacing <= 0 { spacing = time.Nanosecond }
	batchNow := now.UTC()
	for i := 0; i < batchSize; i++ {
		occurredAt := batchNow.Add(-time.Duration(batchSize-1-i) * spacing)
		phase := g.activityPhase(occurredAt)
		g.heartSeq++
		heartBatch.Events = append(heartBatch.Events, HeartRateEvent{EventID: fmt.Sprintf("hr-%s-%06d", g.runID, g.heartSeq), OccurredAt: occurredAt, HeartRateBPM: g.heartRateFor(phase)})
		g.stepsSeq++
		stepBatch.Events = append(stepBatch.Events, StepEvent{EventID: fmt.Sprintf("steps-%s-%06d", g.runID, g.stepsSeq), OccurredAt: occurredAt, StepsDelta: g.stepsFor(phase)})
	}
	return heartBatch, stepBatch
}
func (g *EventGenerator) activityPhase(at time.Time) int {
	elapsed := at.Sub(g.startedAt)
	if elapsed < 0 { elapsed = 0 }
	return int(elapsed/(15*time.Second)) % 3
}
func (g *EventGenerator) heartRateFor(phase int) int {
	var minBPM, maxBPM int
	switch phase {
	case 1: minBPM, maxBPM = 82, 108
	case 2: minBPM, maxBPM = 105, 135
	default: minBPM, maxBPM = 60, 82
	}
	return minBPM + g.rng.Intn(maxBPM-minBPM+1)
}
func (g *EventGenerator) stepsFor(phase int) int {
	switch phase {
	case 1: return 4 + g.rng.Intn(12)
	case 2: return 12 + g.rng.Intn(17)
	default: return g.rng.Intn(3)
	}
}
