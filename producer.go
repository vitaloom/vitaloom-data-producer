package main

import (
	"context"
	"fmt"
	"log"
	"time"
)
type Stats struct {
	RunID string
	StartedAt time.Time
	FinishedAt time.Time
	Cycles int64
	SuccessfulBatches int64
	EventsGenerated int64
	EventsAccepted int64
	EventsDuplicates int64
	RetryAttempts int64
	FailedBatches int64
}
func (s Stats) Duration() time.Duration {
	if s.FinishedAt.IsZero() || s.StartedAt.IsZero() { return 0 }
	return s.FinishedAt.Sub(s.StartedAt)
}
func runProducer(ctx context.Context, cfg Config, client *APIClient, logger *log.Logger, generator *EventGenerator) (Stats, error) {
	startedAt := time.Now()
	stats := Stats{RunID: generator.runID, StartedAt: startedAt}
	var deadline time.Time
	if cfg.Duration > 0 { deadline = startedAt.Add(cfg.Duration) }
	finish := func(err error) (Stats, error) { stats.FinishedAt = time.Now(); return stats, err }
	logger.Printf("producer started run_id=%s api_base_url=%s batch_size=%d interval=%s duration=%s (0s means unlimited) timeout=%s max_retries=%d", stats.RunID, cfg.APIBaseURL, cfg.BatchSize, cfg.Interval, cfg.Duration, cfg.Timeout, cfg.MaxRetries)
	for {
		if ctx.Err() != nil || (!deadline.IsZero() && !time.Now().Before(deadline)) { return finish(nil) }
		now := time.Now().UTC()
		heartBatch, stepBatch := generator.NextBatch(cfg.BatchSize, now, cfg.Interval)
		stats.Cycles++
		stats.EventsGenerated += int64(len(heartBatch.Events) + len(stepBatch.Events))
		heartResult, err := client.SendHeartRateBatch(ctx, heartBatch)
		stats.RetryAttempts += int64(max(heartResult.Attempts-1, 0))
		if err != nil { stats.FailedBatches++; return finish(fmt.Errorf("heart-rate batch failed after %d attempt(s): %w", heartResult.Attempts, err)) }
		stats.SuccessfulBatches++
		stats.EventsAccepted += int64(heartResult.Response.Accepted)
		stats.EventsDuplicates += int64(heartResult.Response.Duplicates)
		stepResult, err := client.SendStepBatch(ctx, stepBatch)
		stats.RetryAttempts += int64(max(stepResult.Attempts-1, 0))
		if err != nil { stats.FailedBatches++; return finish(fmt.Errorf("steps batch failed after %d attempt(s): %w", stepResult.Attempts, err)) }
		stats.SuccessfulBatches++
		stats.EventsAccepted += int64(stepResult.Response.Accepted)
		stats.EventsDuplicates += int64(stepResult.Response.Duplicates)
		if ctx.Err() != nil || (!deadline.IsZero() && !time.Now().Before(deadline)) { return finish(nil) }
		wait := cfg.Interval
		if !deadline.IsZero() { remaining := time.Until(deadline); if remaining < wait { wait = remaining } }
		if wait <= 0 || !waitOrStop(ctx, wait) { return finish(nil) }
	}
}
func waitOrStop(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration); defer timer.Stop()
	select { case <-ctx.Done(): return false; case <-timer.C: return true }
}
func logSummary(logger *log.Logger, stats Stats, runErr error) {
	status := "completed"; if runErr != nil { status = "failed" }
	logger.Printf("producer summary status=%s run_id=%s duration=%s cycles=%d successful_batches=%d generated_events=%d accepted_events=%d duplicate_events=%d retries=%d failed_batches=%d", status, stats.RunID, stats.Duration().Round(time.Millisecond), stats.Cycles, stats.SuccessfulBatches, stats.EventsGenerated, stats.EventsAccepted, stats.EventsDuplicates, stats.RetryAttempts, stats.FailedBatches)
}
