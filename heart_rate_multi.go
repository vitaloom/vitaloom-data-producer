package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type heartRateMultiOptions struct {
	virtualUsers        int
	devicesPerUser      int
	measurementInterval time.Duration
	batchInterval       time.Duration
	rampUp              time.Duration
	duration            time.Duration
}

func hasVirtualUsersFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--virtual-users" || arg == "-virtual-users" || strings.HasPrefix(arg, "--virtual-users=") || strings.HasPrefix(arg, "-virtual-users=") {
			return true
		}
	}
	return false
}

func parseHeartRateMultiUserCommand(args []string, getenv func(string) string) (heartRateMultiOptions, Config, string, error) {
	fs := flag.NewFlagSet("heart-rate", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	vu := fs.Int("virtual-users", 0, "number of deterministic test accounts (1-1000)")
	_ = fs.Int("account", 0, "single test account index; unavailable in multi-user mode")
	devices := fs.Int("devices-per-user", 1, "simulated devices per account (only 1 is supported)")
	measurement := fs.String("measurement-interval", heartRateMeasurementInterval.String(), "time between Heart Rate measurements")
	batch := fs.String("batch-interval", heartRateBatchInterval.String(), "time between Heart Rate batch flushes")
	ramp := fs.String("ramp-up", "0s", "time over which VUs start")
	durationText := fs.String("duration", "0s", "scenario duration including ramp-up")
	if err := fs.Parse(args); err != nil {
		return heartRateMultiOptions{}, Config{}, "", err
	}
	if fs.NArg() != 0 {
		return heartRateMultiOptions{}, Config{}, "", errors.New("heart-rate does not accept positional arguments")
	}
	if *vu < 1 || *vu > 1000 {
		return heartRateMultiOptions{}, Config{}, "", errors.New("--virtual-users must be between 1 and 1000")
	}
	if *devices != 1 {
		return heartRateMultiOptions{}, Config{}, "", errors.New("--devices-per-user currently supports only 1")
	}
	accountSpecified := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "account" {
			accountSpecified = true
		}
	})
	if accountSpecified {
		return heartRateMultiOptions{}, Config{}, "", errors.New("--account cannot be used with --virtual-users")
	}
	parsePositive := func(name, value string) (time.Duration, error) {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("--%s must be a positive duration", name)
		}
		return d, nil
	}
	mi, err := parsePositive("measurement-interval", *measurement)
	if err != nil {
		return heartRateMultiOptions{}, Config{}, "", err
	}
	bi, err := parsePositive("batch-interval", *batch)
	if err != nil {
		return heartRateMultiOptions{}, Config{}, "", err
	}
	ru, err := time.ParseDuration(*ramp)
	if err != nil || ru < 0 {
		return heartRateMultiOptions{}, Config{}, "", errors.New("--ramp-up must be a nonnegative duration")
	}
	dur, err := time.ParseDuration(*durationText)
	if err != nil || dur <= 0 {
		return heartRateMultiOptions{}, Config{}, "", errors.New("multi-user mode requires a positive --duration")
	}
	if ru >= dur {
		return heartRateMultiOptions{}, Config{}, "", errors.New("--ramp-up must be less than --duration")
	}
	for _, arg := range args {
		if arg == "--account" || strings.HasPrefix(arg, "--account=") {
			return heartRateMultiOptions{}, Config{}, "", errors.New("--account cannot be used with --virtual-users")
		}
	}
	cfg, password, err := authConfigFromEnv(getenv)
	if err != nil {
		return heartRateMultiOptions{}, Config{}, "", err
	}
	return heartRateMultiOptions{virtualUsers: *vu, devicesPerUser: *devices, measurementInterval: mi, batchInterval: bi, rampUp: ru, duration: dur}, cfg, password, nil
}

type scenarioCounts struct{ started, completed, failed int }

func runHeartRateMultiUser(parent context.Context, cfg Config, password string, options heartRateMultiOptions, logger *log.Logger) error {
	if err := validateAccountRange(1, options.virtualUsers); err != nil {
		return err
	}
	auth := NewAuthClient(cfg, password, logger)
	client := newAuthenticatedHeartRateClient(cfg, logger)
	// Both API paths share the same transport and connection pool.
	auth.httpClient = client.httpClient
	sessions, err := auth.WarmSessions(parent, 1, options.virtualUsers)
	if err != nil {
		return err
	}
	runID, err := NewRunID(time.Now())
	if err != nil {
		return err
	}
	startedAt := time.Now()
	scenarioCtx, cancel := context.WithTimeout(parent, options.duration)
	defer cancel()
	logger.Printf("heart_rate_scenario_requested run_id=%s requested=%d duration=%s", runID, options.virtualUsers, options.duration)
	var mu sync.Mutex
	counts := scenarioCounts{}
	var firstErr error
	var wg sync.WaitGroup
	for i := 1; i <= options.virtualUsers; i++ {
		index := i
		delay := spreadDuration(options.rampUp, index-1, options.virtualUsers)
		phase := spreadDuration(options.batchInterval, index-1, options.virtualUsers)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if waitContext(scenarioCtx, delay) != nil {
				return
			}
			mu.Lock()
			counts.started++
			mu.Unlock()
			account := deterministicAccount(index)
			vuLogger := log.New(&identityWriter{logger: logger, prefix: fmt.Sprintf("account=%s device=device-loadtest-%04d ", account.Email, index)}, "", 0)
			session := sessions[account.Email]
			generator := NewEventGenerator(runID+fmt.Sprintf("-vu-%04d", index), time.Now(), nil)
			vuClient := newAuthenticatedHeartRateClient(cfg, vuLogger)
			vuClient.httpClient = client.httpClient
			send := func(ctx context.Context, batch authenticatedHeartRateBatch) (authenticatedHeartRateResponse, int, error) {
				if time.Until(session.AccessExpiresAt) <= heartRateRefreshBefore {
					if err := auth.Refresh(ctx, session); err != nil {
						return authenticatedHeartRateResponse{}, 0, fmt.Errorf("could not refresh this VU session: %w", err)
					}
				}
				return vuClient.sendBatch(ctx, session.AccessToken, batch)
			}
			measureTicker := time.NewTicker(options.measurementInterval)
			defer measureTicker.Stop()
			err := runHeartRateScheduleTimed(scenarioCtx, nil, measureTicker.C, nil, options.batchInterval, phase, runID, index, generator, send, vuLogger, func() { measureTicker.Stop() })
			mu.Lock()
			if err != nil {
				counts.failed++
				if firstErr == nil {
					firstErr = err
					cancel()
				}
			} else {
				counts.completed++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	mu.Lock()
	final := counts
	err = firstErr
	mu.Unlock()
	logger.Printf("heart_rate_scenario_completed run_id=%s requested=%d started=%d completed=%d failed=%d elapsed=%s", runID, options.virtualUsers, final.started, final.completed, final.failed, time.Since(startedAt).Round(time.Millisecond))
	return err
}

func spreadDuration(total time.Duration, index, count int) time.Duration {
	if count <= 0 {
		return 0
	}
	return total/time.Duration(count)*time.Duration(index) + (total%time.Duration(count))*time.Duration(index)/time.Duration(count)
}

type identityWriter struct {
	logger *log.Logger
	prefix string
}

func (w *identityWriter) Write(p []byte) (int, error) {
	w.logger.Printf("%s%s", w.prefix, strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}
