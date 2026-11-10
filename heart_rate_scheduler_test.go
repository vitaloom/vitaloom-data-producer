package main

import (
	"context"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartRateSchedulerQueuesMeasurementsDuringBlockedSendAndDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	measurementTicks := make(chan time.Time)
	batchTicks := make(chan time.Time)
	started := make(chan struct{})
	release := make(chan struct{})
	var batches atomic.Int32
	var maximumActive atomic.Int32
	var active atomic.Int32
	var logs strings.Builder
	result := make(chan error, 1)
	go func() {
		result <- runHeartRateScheduleUntil(ctx, nil, measurementTicks, batchTicks, "run-scheduler-test", 1,
			NewEventGenerator("run-scheduler-test", time.Now(), nil), func(sendCtx context.Context, batch authenticatedHeartRateBatch) (authenticatedHeartRateResponse, int, error) {
				nowActive := active.Add(1)
				for old := maximumActive.Load(); nowActive > old && !maximumActive.CompareAndSwap(old, nowActive); old = maximumActive.Load() {
				}
				call := batches.Add(1)
				if call == 1 {
					close(started)
					select {
					case <-release:
					case <-sendCtx.Done():
						active.Add(-1)
						return authenticatedHeartRateResponse{}, 0, sendCtx.Err()
					}
				}
				active.Add(-1)
				results := make([]heartRateItemResult, len(batch.Events))
				for i, event := range batch.Events {
					results[i] = heartRateItemResult{Index: i, EventID: event.EventID, Status: "ACCEPTED"}
				}
				return authenticatedHeartRateResponse{RunID: batch.RunID, Received: len(batch.Events), Accepted: len(batch.Events), Results: results}, 200, nil
			}, log.New(&logs, "", 0))
	}()

	measurementTicks <- time.Now()
	batchTicks <- time.Now()
	<-started
	for i := 0; i < 4; i++ {
		measurementTicks <- time.Now()
	}
	cancel()
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("scheduler returned error: %v", err)
	}
	if got := batches.Load(); got != 2 {
		t.Fatalf("sent batches=%d; want initial send plus one drain batch", got)
	}
	if got := maximumActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent sends=%d; want 1", got)
	}
	for _, expected := range []string{"generated=5 submitted=5 accepted=5", "queued_unsent=0 inflight_unconfirmed=0"} {
		if !strings.Contains(logs.String(), expected) {
			t.Errorf("summary missing %q: %s", expected, logs.String())
		}
	}
}

func TestHeartRateSchedulerReportsBoundedDrainFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	measurementTicks := make(chan time.Time)
	batchTicks := make(chan time.Time)
	started := make(chan struct{})
	var logs strings.Builder
	result := make(chan error, 1)
	go func() {
		result <- runHeartRateScheduleUntil(ctx, nil, measurementTicks, batchTicks, "run-drain-timeout", 1,
			NewEventGenerator("run-drain-timeout", time.Now(), nil), func(sendCtx context.Context, batch authenticatedHeartRateBatch) (authenticatedHeartRateResponse, int, error) {
				close(started)
				<-sendCtx.Done()
				return authenticatedHeartRateResponse{}, 0, sendCtx.Err()
			}, log.New(&logs, "", 0))
	}()
	measurementTicks <- time.Now()
	batchTicks <- time.Now()
	<-started
	cancel()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "shutdown exceeded the 5s drain budget") {
			t.Fatalf("scheduler error = %v; want bounded drain failure", err)
		}
	case <-time.After(heartRateShutdownBudget + time.Second):
		t.Fatal("scheduler did not enforce the drain budget")
	}
	if !strings.Contains(logs.String(), "inflight_unconfirmed=1") {
		t.Fatalf("summary did not report active unconfirmed event: %s", logs.String())
	}
}

func TestHeartRateSchedulerStopsAtOutstandingQueueLimitAndCapsBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	measurementTicks := make(chan time.Time)
	batchTicks := make(chan time.Time)
	started := make(chan struct{})
	release := make(chan struct{})
	var logs strings.Builder
	var sentEvents atomic.Int32
	var maxBatch atomic.Int32
	result := make(chan error, 1)
	go func() {
		result <- runHeartRateScheduleUntil(ctx, nil, measurementTicks, batchTicks, "run-queue-limit", 1,
			NewEventGenerator("run-queue-limit", time.Now(), nil), func(sendCtx context.Context, batch authenticatedHeartRateBatch) (authenticatedHeartRateResponse, int, error) {
				if len(batch.Events) > int(maxBatch.Load()) {
					maxBatch.Store(int32(len(batch.Events)))
				}
				if sentEvents.Load() == 0 {
					close(started)
					select {
					case <-release:
					case <-sendCtx.Done():
						return authenticatedHeartRateResponse{}, 0, sendCtx.Err()
					}
				}
				sentEvents.Add(int32(len(batch.Events)))
				results := make([]heartRateItemResult, len(batch.Events))
				for i, event := range batch.Events {
					results[i] = heartRateItemResult{Index: i, EventID: event.EventID, Status: "ACCEPTED"}
				}
				return authenticatedHeartRateResponse{RunID: batch.RunID, Received: len(batch.Events), Accepted: len(batch.Events), Results: results}, 200, nil
			}, log.New(&logs, "", 0))
	}()
	measurementTicks <- time.Now()
	batchTicks <- time.Now()
	<-started
	for i := 0; i < heartRateQueueLimit; i++ {
		measurementTicks <- time.Now()
	}
	close(release)
	err := <-result
	if err == nil || !strings.Contains(err.Error(), "buffer reached its 1000-event limit") {
		t.Fatalf("scheduler error = %v; want explicit queue-limit error", err)
	}
	if got := maxBatch.Load(); got > maxBatchSize {
		t.Fatalf("largest batch=%d; limit=%d", got, maxBatchSize)
	}
	if got := sentEvents.Load(); got != heartRateQueueLimit {
		t.Fatalf("confirmed event count=%d; want %d", got, heartRateQueueLimit)
	}
	if !strings.Contains(logs.String(), "generated=1000 submitted=1000 accepted=1000") || !strings.Contains(logs.String(), "queued_unsent=0 inflight_unconfirmed=0") {
		t.Fatalf("queue limit summary mismatch: %s", logs.String())
	}
}

func TestParseHeartRateScheduledFlagsAndValidation(t *testing.T) {
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
	_, _, _, measurement, batch, duration, scheduled, err := parseHeartRateCommand([]string{"--account", "1"}, env)
	if err != nil || scheduled || measurement != time.Second || batch != 5*time.Second || duration != 0 {
		t.Fatalf("default parse = %s, %s, %s, scheduled=%t, err=%v", measurement, batch, duration, scheduled, err)
	}
	_, _, _, measurement, batch, duration, scheduled, err = parseHeartRateCommand([]string{"--account", "1", "--measurement-interval", "250ms", "--batch-interval", "2s", "--duration", "3s"}, env)
	if err != nil || !scheduled || measurement != 250*time.Millisecond || batch != 2*time.Second || duration != 3*time.Second {
		t.Fatalf("scheduled parse = %s, %s, %s, scheduled=%t, err=%v", measurement, batch, duration, scheduled, err)
	}
	for _, interval := range []string{"0s", "-1s", "invalid"} {
		_, _, _, _, _, _, _, err := parseHeartRateCommand([]string{"--account", "1", "--measurement-interval", interval}, env)
		if err == nil {
			t.Errorf("invalid measurement interval %q was accepted", interval)
		}
	}
}
