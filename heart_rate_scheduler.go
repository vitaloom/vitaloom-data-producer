package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type heartRateScheduleStats struct {
	Generated       int
	Submitted       int
	Accepted        int
	Duplicates      int
	Rejected        int
	QueuedUnsent    int
	InFlightUnknown int
}

type heartRateSendResult struct {
	batch    authenticatedHeartRateBatch
	response authenticatedHeartRateResponse
	status   int
	err      error
	started  time.Time
}

type heartRateSendFunc func(context.Context, authenticatedHeartRateBatch) (authenticatedHeartRateResponse, int, error)

func runHeartRateScheduled(ctx context.Context, cfg Config, password string, accountIndex int, measurementInterval, batchInterval, duration time.Duration, logger *log.Logger) error {
	return runHeartRateScheduledWithBatchSize(ctx, cfg, password, accountIndex, measurementInterval, batchInterval, duration, maxBatchSize, logger)
}

func runHeartRateScheduledWithBatchSize(ctx context.Context, cfg Config, password string, accountIndex int, measurementInterval, batchInterval, duration time.Duration, batchSize int, logger *log.Logger) error {
	if batchSize < 1 || batchSize > maxBatchSize {
		return fmt.Errorf("Heart Rate batch size must be between 1 and %d", maxBatchSize)
	}
	account := deterministicAccount(accountIndex)
	auth := NewAuthClient(cfg, password, logger)
	session, err := auth.Login(ctx, account.Email)
	if err != nil {
		return accountFailure(account, err)
	}
	runID, err := NewRunID(time.Now())
	if err != nil {
		return err
	}
	generator := NewEventGenerator(runID, time.Now(), nil)
	client := newAuthenticatedHeartRateClient(cfg, logger)
	send := func(sendCtx context.Context, batch authenticatedHeartRateBatch) (authenticatedHeartRateResponse, int, error) {
		if time.Until(session.AccessExpiresAt) <= heartRateRefreshBefore {
			if err := auth.Refresh(sendCtx, session); err != nil {
				return authenticatedHeartRateResponse{}, 0, fmt.Errorf("could not refresh Heart Rate session: %w", err)
			}
		}
		return client.sendBatch(sendCtx, session.AccessToken, batch)
	}
	measureTicker := time.NewTicker(measurementInterval)
	batchTicker := time.NewTicker(batchInterval)
	defer measureTicker.Stop()
	defer batchTicker.Stop()
	measureTicks := measureTicker.C
	batchTicks := batchTicker.C
	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		return runHeartRateScheduleTimedLimit(ctx, timer.C, measureTicks, batchTicks, 0, 0, runID, accountIndex, generator, send, logger, batchSize, func() {
			measureTicker.Stop()
			batchTicker.Stop()
		})
	}
	return runHeartRateScheduleTimedLimit(ctx, nil, measureTicks, batchTicks, 0, 0, runID, accountIndex, generator, send, logger, batchSize, func() {
		measureTicker.Stop()
		batchTicker.Stop()
	})
}

func runHeartRateScheduleUntil(ctx context.Context, duration <-chan time.Time, measurementTicks, batchTicks <-chan time.Time, runID string, accountIndex int, generator *EventGenerator, send heartRateSendFunc, logger *log.Logger) error {
	return runHeartRateScheduleWithStop(ctx, duration, measurementTicks, batchTicks, runID, accountIndex, generator, send, logger, func() {})
}

func runHeartRateScheduleWithStop(ctx context.Context, duration <-chan time.Time, measurementTicks, batchTicks <-chan time.Time, runID string, accountIndex int, generator *EventGenerator, send heartRateSendFunc, logger *log.Logger, stopTickers func()) error {
	return runHeartRateScheduleTimed(ctx, duration, measurementTicks, batchTicks, 0, 0, runID, accountIndex, generator, send, logger, stopTickers)
}

func runHeartRateScheduleTimed(ctx context.Context, duration <-chan time.Time, measurementTicks, batchTicks <-chan time.Time, batchInterval, firstBatchDelay time.Duration, runID string, accountIndex int, generator *EventGenerator, send heartRateSendFunc, logger *log.Logger, stopTickers func()) error {
	return runHeartRateScheduleTimedLimit(ctx, duration, measurementTicks, batchTicks, batchInterval, firstBatchDelay, runID, accountIndex, generator, send, logger, maxBatchSize, stopTickers)
}

func runHeartRateScheduleTimedLimit(ctx context.Context, duration <-chan time.Time, measurementTicks, batchTicks <-chan time.Time, batchInterval, firstBatchDelay time.Duration, runID string, accountIndex int, generator *EventGenerator, send heartRateSendFunc, logger *log.Logger, batchSize int, stopTickers func()) error {
	sendCtx, cancelSend := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSend()
	deviceID := fmt.Sprintf("device-loadtest-%04d", accountIndex)
	queue := make([]HeartRateEvent, 0, heartRateQueueLimit)
	var active *authenticatedHeartRateBatch
	completion := make(chan heartRateSendResult, 1)
	stats := heartRateScheduleStats{}
	var stopErr error
	ctxDone := ctx.Done()
	var drainTimer *time.Timer
	var drainTimeout <-chan time.Time
	var phaseTimer *time.Timer
	var batchTimerTicks <-chan time.Time
	if batchInterval > 0 {
		phaseTimer = time.NewTimer(firstBatchDelay)
		batchTimerTicks = phaseTimer.C
		batchTicks = nil
	}
	draining := false
	flushPending := false
	launch := func(events []HeartRateEvent) {
		batch := authenticatedHeartRateBatch{DeviceID: deviceID, RunID: runID, Events: append([]HeartRateEvent(nil), events...)}
		active = &batch
		stats.Submitted += len(batch.Events)
		started := time.Now()
		go func() {
			response, status, err := send(sendCtx, batch)
			completion <- heartRateSendResult{batch: batch, response: response, status: status, err: err, started: started}
		}()
	}
	startDrain := func(err error) {
		if !draining {
			draining = true
			drainTimer = time.NewTimer(heartRateShutdownBudget)
			drainTimeout = drainTimer.C
			measurementTicks = nil
			batchTicks = nil
			batchTimerTicks = nil
			ctxDone = nil
			stopTickers()
		}
		if err != nil && stopErr == nil {
			stopErr = err
		}
	}
	finish := func(err error) error {
		if drainTimer != nil {
			drainTimer.Stop()
		}
		if phaseTimer != nil {
			phaseTimer.Stop()
		}
		stats.QueuedUnsent = len(queue)
		if active != nil {
			stats.InFlightUnknown = len(active.Events)
		}
		status := "completed"
		if err != nil || stats.QueuedUnsent > 0 || stats.InFlightUnknown > 0 {
			status = "failed"
		}
		logger.Printf("heart_rate_schedule_summary status=%s run_id=%s generated=%d submitted=%d accepted=%d duplicates=%d rejected=%d queued_unsent=%d inflight_unconfirmed=%d", status, runID, stats.Generated, stats.Submitted, stats.Accepted, stats.Duplicates, stats.Rejected, stats.QueuedUnsent, stats.InFlightUnknown)
		if stats.QueuedUnsent > 0 || stats.InFlightUnknown > 0 {
			outcomeErr := fmt.Errorf("Heart Rate shutdown left queued_unsent=%d inflight_unconfirmed=%d", stats.QueuedUnsent, stats.InFlightUnknown)
			if err != nil {
				return errors.Join(err, outcomeErr)
			}
			return outcomeErr
		}
		return err
	}
	for {
		select {
		case <-drainTimeout:
			cancelSend()
			return finish(errors.New("Heart Rate shutdown exceeded the 5s drain budget"))
		default:
		}
		select {
		case <-ctxDone:
			startDrain(nil)
		case <-duration:
			duration = nil
			startDrain(nil)
		default:
		}
		if draining && active == nil {
			if len(queue) == 0 {
				return finish(stopErr)
			}
			n := min(batchSize, len(queue))
			launch(queue[:n])
			queue = queue[n:]
		}
		select {
		case <-ctxDone:
			startDrain(nil)
		case <-duration:
			duration = nil
			startDrain(nil)
		case _, ok := <-measurementTicks:
			if !ok {
				measurementTicks = nil
				continue
			}
			select {
			case <-ctxDone:
				startDrain(nil)
				continue
			case <-duration:
				duration = nil
				startDrain(nil)
				continue
			default:
			}
			if len(queue)+inFlightCount(active) >= heartRateQueueLimit {
				startDrain(errors.New("Heart Rate event buffer reached its 1000-event limit"))
				continue
			}
			createdAt := time.Now().UTC().Truncate(time.Millisecond)
			phase := generator.activityPhase(createdAt)
			event := generator.nextHeartRateEvent(createdAt, phase)
			queue = append(queue, event)
			stats.Generated++
		case <-batchTimerTicks:
			if !draining {
				flushPending = true
				if active == nil && len(queue) > 0 {
					n := min(batchSize, len(queue))
					launch(queue[:n])
					queue = queue[n:]
					flushPending = false
				}
				phaseTimer.Reset(batchInterval)
			} else {
				batchTimerTicks = nil
			}
		case <-batchTicks:
			if !draining {
				flushPending = true
				if active == nil && len(queue) > 0 {
					n := min(batchSize, len(queue))
					launch(queue[:n])
					queue = queue[n:]
					flushPending = false
				}
			}
		case result := <-completion:
			if active == nil {
				continue
			}
			batchCount := len(active.Events)
			if result.err != nil {
				stats.InFlightUnknown = batchCount
				logger.Printf("heart_rate_batch_failed run_id=%s device_id=%s submitted=%d http_status=%d error=%q queued_unsent=%d inflight_unconfirmed=%d", runID, deviceID, batchCount, result.status, result.err.Error(), len(queue), batchCount)
				active = &result.batch
				return finish(fmt.Errorf("Heart Rate batch failed after bounded client retries: %w", result.err))
			}
			stats.Accepted += result.response.Accepted
			stats.Duplicates += result.response.Duplicates
			stats.Rejected += result.response.Rejected
			logger.Printf("heart_rate_batch run_id=%s device_id=%s received=%d submitted=%d accepted=%d duplicates=%d rejected=%d duration=%s", runID, deviceID, result.response.Received, batchCount, result.response.Accepted, result.response.Duplicates, result.response.Rejected, time.Since(result.started).Round(time.Millisecond))
			active = nil
			if !draining && flushPending && len(queue) > 0 {
				n := min(maxBatchSize, len(queue))
				launch(queue[:n])
				queue = queue[n:]
				flushPending = false
			}
		case <-drainTimeout:
			cancelSend()
			return finish(errors.New("Heart Rate shutdown exceeded the 5s drain budget"))
		}
	}
}

func inFlightCount(batch *authenticatedHeartRateBatch) int {
	if batch == nil {
		return 0
	}
	return len(batch.Events)
}
