package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	stepsMeasurementInterval = time.Second
	stepsQueueLimit          = 1000
	stepsShutdownBudget      = 5 * time.Second
)

type stepsCommandOptions struct {
	accountIndex  int
	batchSize     int
	batchInterval time.Duration
	duration      time.Duration
}

type authenticatedStepsBatch struct {
	DeviceID string      `json:"deviceId"`
	RunID    string      `json:"runId"`
	Events   []StepEvent `json:"events"`
}

type stepsItemError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type stepsItemResult struct {
	Index   int             `json:"index"`
	EventID string          `json:"eventId"`
	Status  string          `json:"status"`
	Error   *stepsItemError `json:"error,omitempty"`
}

type authenticatedStepsResponse struct {
	RunID      string            `json:"runId"`
	Received   int               `json:"received"`
	Accepted   int               `json:"accepted"`
	Duplicates int               `json:"duplicates"`
	Rejected   int               `json:"rejected"`
	Results    []stepsItemResult `json:"results"`
}

type stepsClient struct {
	baseURL    string
	httpClient *http.Client
	maxRetries int
	retryDelay time.Duration
	logger     *log.Logger
}

func newStepsClient(cfg Config, logger *log.Logger) *stepsClient {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &stepsClient{baseURL: strings.TrimRight(cfg.APIBaseURL, "/"), httpClient: newAuthHTTPClient(cfg.Timeout), maxRetries: cfg.MaxRetries, retryDelay: cfg.RetryDelay, logger: logger}
}

func (c *stepsClient) sendBatch(ctx context.Context, token string, batch authenticatedStepsBatch) (authenticatedStepsResponse, int, error) {
	if len(batch.Events) < 1 || len(batch.Events) > maxBatchSize {
		return authenticatedStepsResponse{}, 0, fmt.Errorf("Steps batch must contain between 1 and %d events", maxBatchSize)
	}
	body, err := json.Marshal(batch)
	if err != nil {
		return authenticatedStepsResponse{}, 0, errors.New("could not encode Steps batch")
	}
	maxAttempts := c.maxRetries + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/steps/batch", bytes.NewReader(body))
		if err != nil {
			return authenticatedStepsResponse{}, 0, errors.New("could not prepare Steps request")
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		response, requestErr := c.httpClient.Do(request)
		if requestErr != nil {
			lastErr = errors.New("Steps request failed")
			if ctx.Err() != nil || attempt == maxAttempts {
				return authenticatedStepsResponse{}, 0, lastErr
			}
			if err := waitContext(ctx, c.retryDelay); err != nil {
				return authenticatedStepsResponse{}, 0, err
			}
			continue
		}
		status := response.StatusCode
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes+1))
		_ = response.Body.Close()
		if readErr != nil || len(responseBody) > maxResponseBodyBytes {
			lastErr = errors.New("could not read valid Steps response")
			if attempt == maxAttempts {
				return authenticatedStepsResponse{}, status, lastErr
			}
			if err := waitContext(ctx, c.retryDelay); err != nil {
				return authenticatedStepsResponse{}, status, err
			}
			continue
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("POST /api/steps/batch returned HTTP %d", status)
			retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
			if !retryable || attempt == maxAttempts {
				return authenticatedStepsResponse{}, status, lastErr
			}
			if err := waitContext(ctx, c.retryDelay); err != nil {
				return authenticatedStepsResponse{}, status, err
			}
			continue
		}
		var result authenticatedStepsResponse
		if err := json.Unmarshal(responseBody, &result); err != nil {
			lastErr = fmt.Errorf("decode Steps response: %w", err)
		} else if err := validateStepsResponse(batch, result); err != nil {
			lastErr = err
		} else {
			return result, status, nil
		}
		if attempt == maxAttempts {
			return authenticatedStepsResponse{}, status, lastErr
		}
		if err := waitContext(ctx, c.retryDelay); err != nil {
			return authenticatedStepsResponse{}, status, err
		}
	}
	return authenticatedStepsResponse{}, 0, errors.New("Steps request exhausted retries")
}

func validateStepsResponse(batch authenticatedStepsBatch, response authenticatedStepsResponse) error {
	if response.RunID != batch.RunID || response.Received != len(batch.Events) ||
		response.Accepted < 0 || response.Duplicates < 0 || response.Rejected < 0 ||
		response.Received != response.Accepted+response.Duplicates+response.Rejected || len(response.Results) != len(batch.Events) {
		return errors.New("Steps response counters or result count did not match the submitted batch")
	}
	accepted, duplicates, rejected := 0, 0, 0
	for i, result := range response.Results {
		if result.Index != i || result.EventID != batch.Events[i].EventID {
			return fmt.Errorf("Steps response item %d did not match the submitted item order and identity", i)
		}
		switch result.Status {
		case "ACCEPTED":
			if result.Error != nil {
				return fmt.Errorf("Steps response item %d included an error for ACCEPTED status", i)
			}
			accepted++
		case "DUPLICATE":
			if result.Error != nil {
				return fmt.Errorf("Steps response item %d included an error for DUPLICATE status", i)
			}
			duplicates++
		case "REJECTED":
			if result.Error == nil || strings.TrimSpace(result.Error.Code) == "" {
				return fmt.Errorf("Steps response item %d did not include an error code for REJECTED status", i)
			}
			rejected++
		default:
			return fmt.Errorf("Steps response item %d had an unknown status", i)
		}
	}
	if accepted != response.Accepted || duplicates != response.Duplicates || rejected != response.Rejected {
		return errors.New("Steps response item statuses did not match the response counters")
	}
	return nil
}

func parseStepsCommand(args []string, getenv func(string) string) (stepsCommandOptions, Config, string, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	fs := flag.NewFlagSet("step", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: producer step [--account INDEX --batch-size COUNT --batch-interval DURATION --duration DURATION]")
		fmt.Fprintln(fs.Output(), "Sends authenticated Steps increments every second to the configured Nginx ingress.")
	}
	account := fs.Int("account", 1, "provisioned deterministic test account index (1-1000)")
	batchSize := fs.Int("batch-size", envInt(getenv("VITALOOM_BATCH_SIZE"), 5), "maximum events per Steps batch (1-500)")
	batchText := fs.String("batch-interval", envString(getenv("VITALOOM_BATCH_INTERVAL"), envString(getenv("VITALOOM_INTERVAL"), "5s")), "time between Steps batch flushes")
	durationText := fs.String("duration", envString(getenv("VITALOOM_DURATION"), "0s"), "optional run duration; 0s runs until Ctrl+C")
	if err := fs.Parse(args); err != nil {
		return stepsCommandOptions{}, Config{}, "", err
	}
	if fs.NArg() != 0 {
		return stepsCommandOptions{}, Config{}, "", errors.New("step does not accept positional arguments")
	}
	if err := validateAccountRange(*account, *account); err != nil {
		return stepsCommandOptions{}, Config{}, "", errors.New("--account must be between 1 and 1000")
	}
	if *batchSize < 1 || *batchSize > maxBatchSize {
		return stepsCommandOptions{}, Config{}, "", fmt.Errorf("--batch-size must be between 1 and %d", maxBatchSize)
	}
	batchInterval, err := time.ParseDuration(*batchText)
	if err != nil || batchInterval <= 0 {
		return stepsCommandOptions{}, Config{}, "", errors.New("--batch-interval must be a positive duration")
	}
	duration, err := time.ParseDuration(*durationText)
	if err != nil || duration < 0 {
		return stepsCommandOptions{}, Config{}, "", errors.New("--duration must be zero or a positive duration")
	}
	cfg, password, err := authConfigFromEnv(getenv)
	if err != nil {
		return stepsCommandOptions{}, Config{}, "", err
	}
	return stepsCommandOptions{accountIndex: *account, batchSize: *batchSize, batchInterval: batchInterval, duration: duration}, cfg, password, nil
}

func envString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func envInt(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed
}

func parseCombinedCommand(args []string, getenv func(string) string) (stepsCommandOptions, Config, string, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	account := 1
	apiBaseURLSpecified := strings.TrimSpace(getenv("VITALOOM_API_BASE_URL")) != ""
	configArgs := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--api-base-url" || arg == "-api-base-url" || strings.HasPrefix(arg, "--api-base-url=") || strings.HasPrefix(arg, "-api-base-url=") {
			apiBaseURLSpecified = true
		}
		if arg == "--account" || arg == "-account" {
			if i+1 >= len(args) {
				return stepsCommandOptions{}, Config{}, "", errors.New("--account requires an index")
			}
			i++
			parsed, err := strconv.Atoi(args[i])
			if err != nil {
				return stepsCommandOptions{}, Config{}, "", errors.New("--account must be an integer between 1 and 1000")
			}
			account = parsed
			continue
		}
		if strings.HasPrefix(arg, "--account=") || strings.HasPrefix(arg, "-account=") {
			value := strings.SplitN(arg, "=", 2)[1]
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return stepsCommandOptions{}, Config{}, "", errors.New("--account must be an integer between 1 and 1000")
			}
			account = parsed
			continue
		}
		configArgs = append(configArgs, arg)
	}
	if err := validateAccountRange(account, account); err != nil {
		return stepsCommandOptions{}, Config{}, "", errors.New("--account must be between 1 and 1000")
	}
	parsedCfg, err := ParseConfig(configArgs, getenv)
	if err != nil {
		return stepsCommandOptions{}, Config{}, "", err
	}
	if !apiBaseURLSpecified {
		return stepsCommandOptions{}, Config{}, "", errors.New("VITALOOM_API_BASE_URL must be set to an authorized test Nginx URL")
	}
	baseURL := parsedCfg.APIBaseURL
	cfg, password, err := authConfigFromEnv(func(key string) string {
		if key == "VITALOOM_API_BASE_URL" {
			return baseURL
		}
		return getenv(key)
	})
	if err != nil {
		return stepsCommandOptions{}, Config{}, "", err
	}
	// Keep the legacy interval/duration/batch-size knobs meaningful in combined mode.
	cfg.BatchSize = parsedCfg.BatchSize
	cfg.Interval = parsedCfg.Interval
	cfg.Duration = parsedCfg.Duration
	return stepsCommandOptions{accountIndex: account, batchSize: cfg.BatchSize, batchInterval: cfg.Interval, duration: cfg.Duration}, cfg, password, nil
}

func runStepsScheduled(ctx context.Context, cfg Config, password string, options stepsCommandOptions, logger *log.Logger) error {
	account := deterministicAccount(options.accountIndex)
	auth := NewAuthClient(cfg, password, logger)
	session, err := auth.Login(ctx, account.Email)
	if err != nil {
		return accountFailure(account, err)
	}
	runID, err := NewRunID(time.Now())
	if err != nil {
		return err
	}
	generator := NewStepEventGenerator(runID, time.Now(), nil)
	client := newStepsClient(cfg, logger)
	send := func(sendCtx context.Context, batch authenticatedStepsBatch) (authenticatedStepsResponse, int, error) {
		if time.Until(session.AccessExpiresAt) <= heartRateRefreshBefore {
			if err := auth.Refresh(sendCtx, session); err != nil {
				return authenticatedStepsResponse{}, 0, fmt.Errorf("could not refresh Steps session: %w", err)
			}
		}
		return client.sendBatch(sendCtx, session.AccessToken, batch)
	}
	logger.Printf("steps_started account=%s run_id=%s device_id=%s measurement_interval=%s batch_interval=%s duration=%s", account.Email, runID, stepsDeviceID(options.accountIndex), stepsMeasurementInterval, options.batchInterval, options.duration)
	return runStepsSchedule(ctx, options, runID, generator, send, logger)
}

func stepsDeviceID(accountIndex int) string {
	return fmt.Sprintf("steps-device-loadtest-%04d", accountIndex)
}

type stepsSendResult struct {
	batch    authenticatedStepsBatch
	response authenticatedStepsResponse
	status   int
	err      error
}

type stepsSendFunc func(context.Context, authenticatedStepsBatch) (authenticatedStepsResponse, int, error)

func runStepsSchedule(ctx context.Context, options stepsCommandOptions, runID string, generator *StepEventGenerator, send stepsSendFunc, logger *log.Logger) error {
	if options.batchSize < 1 || options.batchSize > maxBatchSize || options.batchInterval <= 0 {
		return errors.New("Steps batch size must be 1-500 and batch interval must be positive")
	}
	measureTicker := time.NewTicker(stepsMeasurementInterval)
	flushTicker := time.NewTicker(options.batchInterval)
	defer measureTicker.Stop()
	defer flushTicker.Stop()
	sendCtx, cancelSend := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSend()
	queue := make([]StepEvent, 0, min(options.batchSize, stepsQueueLimit))
	var active *authenticatedStepsBatch
	completion := make(chan stepsSendResult, 1)
	var duration <-chan time.Time
	var durationTimer *time.Timer
	if options.duration > 0 {
		durationTimer = time.NewTimer(options.duration)
		duration = durationTimer.C
		defer durationTimer.Stop()
	}
	var drainTimer *time.Timer
	var drainTimeout <-chan time.Time
	draining := false
	var stopErr error
	var generated, submitted, accepted, duplicates, rejected int
	launch := func(events []StepEvent) {
		batch := authenticatedStepsBatch{DeviceID: stepsDeviceID(options.accountIndex), RunID: runID, Events: append([]StepEvent(nil), events...)}
		active = &batch
		submitted += len(events)
		go func() {
			response, status, err := send(sendCtx, batch)
			completion <- stepsSendResult{batch: batch, response: response, status: status, err: err}
		}()
	}
	startDrain := func(err error) {
		if !draining {
			draining = true
			drainTimer = time.NewTimer(stepsShutdownBudget)
			drainTimeout = drainTimer.C
			measureTicker.Stop()
			flushTicker.Stop()
		}
		if err != nil && stopErr == nil {
			stopErr = err
		}
	}
	finish := func(err error) error {
		if drainTimer != nil {
			drainTimer.Stop()
		}
		inflight := 0
		if active != nil {
			inflight = len(active.Events)
		}
		status := "completed"
		if err != nil || len(queue) > 0 || inflight > 0 {
			status = "failed"
		}
		logger.Printf("steps_summary status=%s run_id=%s generated=%d submitted=%d accepted=%d duplicates=%d rejected=%d queued_unsent=%d inflight_unconfirmed=%d", status, runID, generated, submitted, accepted, duplicates, rejected, len(queue), inflight)
		if len(queue) > 0 || inflight > 0 {
			outcomeErr := fmt.Errorf("Steps shutdown left queued_unsent=%d inflight_unconfirmed=%d", len(queue), inflight)
			if err != nil {
				return errors.Join(err, outcomeErr)
			}
			return outcomeErr
		}
		return err
	}
	for {
		select {
		case <-ctx.Done():
			startDrain(nil)
		default:
		}
		select {
		case <-drainTimeout:
			cancelSend()
			return finish(errors.New("Steps shutdown exceeded the 5s drain budget"))
		default:
		}
		if draining && active == nil {
			if len(queue) == 0 {
				return finish(stopErr)
			}
			n := min(options.batchSize, len(queue))
			launch(queue[:n])
			queue = queue[n:]
		}
		select {
		case <-ctx.Done():
			startDrain(nil)
		case <-duration:
			duration = nil
			startDrain(nil)
		case <-measureTicker.C:
			if draining {
				continue
			}
			if len(queue)+inFlightSteps(active) >= stepsQueueLimit {
				startDrain(errors.New("Steps event buffer reached its 1000-event limit"))
				continue
			}
			at := time.Now().UTC().Truncate(time.Millisecond)
			if event, ok := generator.Next(at); ok {
				queue = append(queue, event)
				generated++
			}
		case <-flushTicker.C:
			if !draining && active == nil && len(queue) > 0 {
				n := min(options.batchSize, len(queue))
				launch(queue[:n])
				queue = queue[n:]
			}
		case result := <-completion:
			if active == nil {
				continue
			}
			if result.err != nil {
				active = &result.batch
				logger.Printf("steps_batch_failed run_id=%s device_id=%s submitted=%d http_status=%d error=%q inflight_unconfirmed=%d", runID, result.batch.DeviceID, len(result.batch.Events), result.status, result.err.Error(), len(result.batch.Events))
				return finish(fmt.Errorf("Steps batch failed after bounded client retries: %w", result.err))
			}
			accepted += result.response.Accepted
			duplicates += result.response.Duplicates
			rejected += result.response.Rejected
			logger.Printf("steps_batch run_id=%s device_id=%s received=%d accepted=%d duplicates=%d rejected=%d", runID, result.batch.DeviceID, result.response.Received, result.response.Accepted, result.response.Duplicates, result.response.Rejected)
			for _, item := range result.response.Results {
				if item.Status == "REJECTED" {
					code := ""
					if item.Error != nil {
						code = item.Error.Code
					}
					logger.Printf("steps_rejected index=%d event_id=%s error_code=%s", item.Index, item.EventID, code)
				}
			}
			active = nil
		}
	}
}

func inFlightSteps(batch *authenticatedStepsBatch) int {
	if batch == nil {
		return 0
	}
	return len(batch.Events)
}

func runAuthenticatedCombined(ctx context.Context, cfg Config, password string, options stepsCommandOptions, logger *log.Logger) error {
	combinedCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		results <- runHeartRateScheduledWithBatchSize(combinedCtx, cfg, password, options.accountIndex, heartRateMeasurementInterval, options.batchInterval, options.duration, options.batchSize, logger)
	}()
	go func() {
		stepsOptions := options
		results <- runStepsScheduled(combinedCtx, cfg, password, stepsOptions, logger)
	}()
	firstErr := <-results
	if firstErr != nil {
		cancel()
	}
	secondErr := <-results
	return errors.Join(firstErr, secondErr)
}
