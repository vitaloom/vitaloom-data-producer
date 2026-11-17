package main

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"time"
)

type HeartRateEvent struct {
	EventID      string    `json:"eventId"`
	OccurredAt   time.Time `json:"occurredAt"`
	HeartRateBPM int       `json:"heartRateBpm"`
}

type StepEvent struct {
	EventID    string     `json:"eventId"`
	OccurredAt *time.Time `json:"occurredAt,omitempty"`
	StartAt    *time.Time `json:"startAt,omitempty"`
	EndAt      *time.Time `json:"endAt,omitempty"`
	StepsDelta int        `json:"stepsDelta"`
}

type EventGenerator struct {
	runID     string
	startedAt time.Time
	rng       *mathrand.Rand
	heartSeq  uint64
}

type StepEventGenerator struct {
	runID string
	rng   *mathrand.Rand
	seq   uint64
}

func NewEventGenerator(runID string, startedAt time.Time, rng *mathrand.Rand) *EventGenerator {
	if rng == nil {
		rng = mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	}
	return &EventGenerator{runID: runID, startedAt: startedAt.UTC(), rng: rng}
}

func NewStepEventGenerator(runID string, rng *mathrand.Rand) *StepEventGenerator {
	if rng == nil {
		rng = mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	}
	return &StepEventGenerator{runID: runID, rng: rng}
}

func NewRunID(now time.Time) (string, error) {
	var random [6]byte
	if _, err := cryptorand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate run ID: %w", err)
	}
	return fmt.Sprintf("run-%s-%s", now.UTC().Format("20060102T150405Z"), hex.EncodeToString(random[:])), nil
}

func (g *EventGenerator) activityPhase(at time.Time) int {
	elapsed := at.Sub(g.startedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	return int(elapsed/(15*time.Second)) % 3
}

func (g *EventGenerator) nextHeartRateEvent(occurredAt time.Time, phase int) HeartRateEvent {
	g.heartSeq++
	return HeartRateEvent{EventID: fmt.Sprintf("hr-%s-%06d", g.runID, g.heartSeq), OccurredAt: occurredAt.UTC().Truncate(time.Millisecond), HeartRateBPM: g.heartRateFor(phase)}
}

func (g *EventGenerator) heartRateFor(phase int) int {
	var minBPM, maxBPM int
	switch phase {
	case 1:
		minBPM, maxBPM = 82, 108
	case 2:
		minBPM, maxBPM = 105, 135
	default:
		minBPM, maxBPM = 60, 82
	}
	return minBPM + g.rng.Intn(maxBPM-minBPM+1)
}

func (g *StepEventGenerator) Next(at time.Time) (StepEvent, bool) {
	delta := g.NextDelta()
	return makeStepEvent(g.runID, &g.seq, delta, at)
}

func (g *StepEventGenerator) NextDelta() int {
	delta := 0
	if g.seq == 0 {
		delta = 1 + g.rng.Intn(5)
	} else {
		delta = g.rng.Intn(6)
	}
	return delta
}

func (g *StepEventGenerator) NextInterval(startAt, endAt time.Time, delta int) (StepEvent, bool) {
	if delta < 1 || delta > 5*int(endAt.Sub(startAt)/time.Second) || !wholeSecondInterval(startAt, endAt) {
		return StepEvent{}, false
	}
	start := startAt.UTC().Truncate(time.Millisecond)
	end := endAt.UTC().Truncate(time.Millisecond)
	g.seq++
	return StepEvent{EventID: fmt.Sprintf("steps-%s-%06d", g.runID, g.seq), StartAt: &start, EndAt: &end, StepsDelta: delta}, true
}

func makeStepEvent(runID string, sequence *uint64, delta int, at time.Time) (StepEvent, bool) {
	if delta < 1 || delta > 5 {
		return StepEvent{}, false
	}
	*sequence++
	occurredAt := at.UTC().Truncate(time.Millisecond)
	return StepEvent{EventID: fmt.Sprintf("steps-%s-%06d", runID, *sequence), OccurredAt: &occurredAt, StepsDelta: delta}, true
}

func wholeSecondInterval(startAt, endAt time.Time) bool {
	duration := endAt.Sub(startAt)
	return !startAt.IsZero() && !endAt.IsZero() && duration >= time.Second && duration <= time.Hour && duration%time.Second == 0 && startAt.Nanosecond()%int(time.Second) == 0 && endAt.Nanosecond()%int(time.Second) == 0
}
