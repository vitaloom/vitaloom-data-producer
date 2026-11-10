package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	observationInterval = time.Second
	queueLimit          = 1000
	shutdownBudget      = 5 * time.Second
	refreshBefore       = 30 * time.Second
)

type MetricSelection uint8

const (
	MetricHeartRate MetricSelection = 1 << iota
	MetricSteps
	MetricBoth = MetricHeartRate | MetricSteps
)

type RunPlan struct {
	Metrics        MetricSelection
	AccountIndex   int
	VirtualUsers   int
	DevicesPerUser int
	RampUp         time.Duration
	BatchSize      int
	BatchInterval  time.Duration
	Duration       time.Duration
	Config         Config
}

func ParseRunPlan(args []string, getenv func(string) string) (RunPlan, string, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	metrics := MetricBoth
	if len(args) > 0 {
		switch args[0] {
		case "step":
			metrics = MetricSteps
			args = args[1:]
		case "heart-rate":
			metrics = MetricHeartRate
			args = args[1:]
		}
	}
	fs := flag.NewFlagSet("vitaloom-producer", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: producer [step|heart-rate] [options]")
		fmt.Fprintln(fs.Output(), "Without a selector, both authenticated metric streams run. Measurements are sampled once per second.")
		fmt.Fprintln(fs.Output(), "Use --batch-interval (or its --interval alias) to set the shared flush cadence.")
		fmt.Fprintln(fs.Output(), "Options:")
		fs.PrintDefaults()
	}
	accountDefault, err := envIntValue(getenv, "VITALOOM_ACCOUNT", 1)
	if err != nil {
		return RunPlan{}, "", err
	}
	usersDefault, err := envIntValue(getenv, "VITALOOM_VIRTUAL_USERS", 0)
	if err != nil {
		return RunPlan{}, "", err
	}
	devicesDefault, err := envIntValue(getenv, "VITALOOM_DEVICES_PER_USER", 1)
	if err != nil {
		return RunPlan{}, "", err
	}
	batchDefault, err := envIntValue(getenv, "VITALOOM_BATCH_SIZE", 5)
	if err != nil {
		return RunPlan{}, "", err
	}
	account := fs.Int("account", accountDefault, "provisioned account index (1-1000; mutually exclusive with --virtual-users)")
	users := fs.Int("virtual-users", usersDefault, "number of provisioned accounts to run (1-1000)")
	devices := fs.Int("devices-per-user", devicesDefault, "simulated devices per account (only 1 is supported)")
	rampText := fs.String("ramp-up", envString(getenv("VITALOOM_RAMP_UP"), "0s"), "time over which virtual users start")
	batchSize := fs.Int("batch-size", batchDefault, "maximum events per request (1-500)")
	batchIntervalFlag := fs.String("batch-interval", "", "shared flush cadence (default 5s; takes precedence over --interval)")
	intervalAliasFlag := fs.String("interval", "", "compatibility alias for --batch-interval")
	durationText := fs.String("duration", envString(getenv("VITALOOM_DURATION"), "0s"), "run duration; 0s continues until stopped")
	apiBaseURL := fs.String("api-base-url", strings.TrimSpace(getenv("VITALOOM_API_BASE_URL")), "authorized test Nginx base URL")
	timeoutText := fs.String("timeout", envString(getenv("VITALOOM_TIMEOUT"), "5s"), "timeout per HTTP attempt")
	retryDefault, err := envIntValue(getenv, "VITALOOM_MAX_RETRIES", 3)
	if err != nil {
		return RunPlan{}, "", err
	}
	retries := fs.Int("max-retries", retryDefault, "retries after the first request (0-10)")
	retryDelayText := fs.String("retry-delay", envString(getenv("VITALOOM_RETRY_DELAY"), "500ms"), "delay between retries")
	if err := fs.Parse(args); err != nil {
		return RunPlan{}, "", err
	}
	if fs.NArg() != 0 {
		return RunPlan{}, "", fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	visited := map[string]bool{}
	fs.Visit(func(value *flag.Flag) { visited[value.Name] = true })
	virtualUsers := *users
	if virtualUsers < 0 || virtualUsers > maxAccountIndex {
		return RunPlan{}, "", fmt.Errorf("--virtual-users must be between 1 and %d when specified", maxAccountIndex)
	}
	if strings.TrimSpace(getenv("VITALOOM_VIRTUAL_USERS")) != "" && virtualUsers == 0 {
		return RunPlan{}, "", fmt.Errorf("VITALOOM_VIRTUAL_USERS must be between 1 and %d", maxAccountIndex)
	}
	if virtualUsers > 0 && visited["account"] {
		return RunPlan{}, "", errors.New("--account cannot be used with --virtual-users")
	}
	if virtualUsers == 0 && (visited["virtual-users"] || *users != 0) {
		return RunPlan{}, "", fmt.Errorf("--virtual-users must be between 1 and %d", maxAccountIndex)
	}
	if *devices != 1 {
		return RunPlan{}, "", errors.New("--devices-per-user currently supports only 1")
	}
	accountIndex := *account
	if virtualUsers > 0 {
		accountIndex = 1
	} else if err := validateAccountRange(accountIndex, accountIndex); err != nil {
		return RunPlan{}, "", errors.New("--account must be between 1 and 1000")
	}
	batchSizeValue := *batchSize
	if batchSizeValue < 1 || batchSizeValue > maxBatchSize {
		return RunPlan{}, "", fmt.Errorf("--batch-size must be between 1 and %d", maxBatchSize)
	}
	batchIntervalValue, err := resolveBatchInterval(visited, *batchIntervalFlag, *intervalAliasFlag, getenv)
	if err != nil {
		return RunPlan{}, "", err
	}
	duration, err := time.ParseDuration(strings.TrimSpace(*durationText))
	if err != nil || duration < 0 {
		return RunPlan{}, "", errors.New("--duration must be zero or a positive duration")
	}
	rampUp, err := time.ParseDuration(strings.TrimSpace(*rampText))
	if err != nil || rampUp < 0 {
		return RunPlan{}, "", errors.New("--ramp-up must be a nonnegative duration")
	}
	if virtualUsers > 1 && duration > 0 && rampUp >= duration {
		return RunPlan{}, "", errors.New("--ramp-up must be less than --duration")
	}
	timeout, err := time.ParseDuration(strings.TrimSpace(*timeoutText))
	if err != nil || timeout <= 0 {
		return RunPlan{}, "", errors.New("--timeout must be a positive duration")
	}
	retryDelay, err := time.ParseDuration(strings.TrimSpace(*retryDelayText))
	if err != nil || retryDelay < 0 {
		return RunPlan{}, "", errors.New("--retry-delay must be a nonnegative duration")
	}
	if *retries < 0 || *retries > 10 {
		return RunPlan{}, "", errors.New("--max-retries must be between 0 and 10")
	}
	parsedURL, err := url.Parse(strings.TrimSpace(*apiBaseURL))
	if err != nil || parsedURL == nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return RunPlan{}, "", errors.New("--api-base-url must be an authorized absolute http:// or https:// Nginx URL without credentials, query, or fragment")
	}
	password := getenv("VITALOOM_TEST_PASSWORD")
	if len([]rune(password)) < 8 {
		return RunPlan{}, "", errors.New("VITALOOM_TEST_PASSWORD must contain at least eight characters")
	}
	plan := RunPlan{
		Metrics: metrics, AccountIndex: accountIndex, VirtualUsers: virtualUsers, DevicesPerUser: *devices,
		RampUp: rampUp, BatchSize: batchSizeValue, BatchInterval: batchIntervalValue, Duration: duration,
		Config: Config{APIBaseURL: strings.TrimRight(strings.TrimSpace(*apiBaseURL), "/"), BatchSize: batchSizeValue, Interval: batchIntervalValue, Duration: duration, Timeout: timeout, MaxRetries: *retries, RetryDelay: retryDelay},
	}
	if plan.VirtualUsers == 0 {
		plan.VirtualUsers = 1
	}
	return plan, password, nil
}

func envIntValue(getenv func(string) string, name string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return parsed, nil
}

func envString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func resolveBatchInterval(visited map[string]bool, canonicalFlag, aliasFlag string, getenv func(string) string) (time.Duration, error) {
	value := ""
	switch {
	case visited["batch-interval"]:
		value = canonicalFlag
	case visited["interval"]:
		value = aliasFlag
	case strings.TrimSpace(getenv("VITALOOM_BATCH_INTERVAL")) != "":
		value = getenv("VITALOOM_BATCH_INTERVAL")
	case strings.TrimSpace(getenv("VITALOOM_INTERVAL")) != "":
		value = getenv("VITALOOM_INTERVAL")
	default:
		value = "5s"
	}
	interval, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil || interval <= 0 {
		return 0, errors.New("--batch-interval must be a positive duration")
	}
	return interval, nil
}

func runPlan(ctx context.Context, plan RunPlan, password string, logger *log.Logger) error {
	if plan.VirtualUsers < 1 || plan.VirtualUsers > maxAccountIndex {
		return fmt.Errorf("virtual user count must be between 1 and %d", maxAccountIndex)
	}
	from, to := plan.AccountIndex, plan.AccountIndex
	if plan.VirtualUsers > 1 {
		from, to = 1, plan.VirtualUsers
	}
	auth := NewAuthClient(plan.Config, password, logger)
	heartClient := newAuthenticatedHeartRateClient(plan.Config, logger)
	stepsClient := newStepsClient(plan.Config, logger)
	stepsClient.httpClient = heartClient.httpClient
	auth.httpClient = heartClient.httpClient
	sessions, err := auth.WarmSessions(ctx, from, to)
	if err != nil {
		return err
	}
	runID, err := NewRunID(time.Now())
	if err != nil {
		return err
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if plan.Duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, plan.Duration)
		defer cancel()
	}
	workerCtx, stopWorkers := context.WithCancel(runCtx)
	defer stopWorkers()
	count := to - from + 1
	results := make(chan error, count)
	for index := from; index <= to; index++ {
		index := index
		account := deterministicAccount(index)
		session := sessions[account.Email]
		launchDelay := spreadDuration(plan.RampUp, index-from, count)
		flushPhase := spreadDuration(plan.BatchInterval, index-from, count)
		go func() {
			vuLogger := log.New(&identityWriter{logger: logger, prefix: fmt.Sprintf("account=%s ", account.Email)}, "", 0)
			results <- runVirtualUser(workerCtx, plan, runID, account, session, auth, heartClient, stepsClient, launchDelay, plan.BatchInterval+flushPhase, vuLogger)
		}()
	}
	var firstErr error
	for i := 0; i < count; i++ {
		if workerErr := <-results; workerErr != nil && firstErr == nil {
			firstErr = workerErr
			stopWorkers()
		}
	}
	return firstErr
}

type sessionToken struct {
	mu      sync.Mutex
	auth    *AuthClient
	session *Session
}

func (s *sessionToken) accessToken(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Until(s.session.AccessExpiresAt) <= refreshBefore {
		if err := s.auth.Refresh(ctx, s.session); err != nil {
			return "", err
		}
	}
	return s.session.AccessToken, nil
}

type metricSendResult struct {
	metric   MetricSelection
	hrBatch  authenticatedHeartRateBatch
	stBatch  authenticatedStepsBatch
	hrResult authenticatedHeartRateResponse
	stResult authenticatedStepsResponse
	status   int
	err      error
}

func runVirtualUser(ctx context.Context, plan RunPlan, runID string, account Account, session *Session, auth *AuthClient, heartClient *authenticatedHeartRateClient, stepsClient *stepsClient, launchDelay, firstFlushDelay time.Duration, logger *log.Logger) error {
	if err := waitContext(ctx, launchDelay); err != nil {
		return nil
	}
	vuRunID := fmt.Sprintf("%s-vu-%04d", runID, account.Index)
	token := &sessionToken{auth: auth, session: session}
	var heartGenerator *EventGenerator
	var stepsGenerator *StepEventGenerator
	started := time.Now()
	if plan.Metrics&MetricHeartRate != 0 {
		heartGenerator = NewEventGenerator(vuRunID, started, nil)
	}
	if plan.Metrics&MetricSteps != 0 {
		stepsGenerator = NewStepEventGenerator(vuRunID, nil)
	}
	deviceID := fmt.Sprintf("device-loadtest-%04d", account.Index)
	stepsID := stepsDeviceID(account.Index)
	measurementTicker := time.NewTicker(observationInterval)
	defer measurementTicker.Stop()
	firstFlush := time.NewTimer(firstFlushDelay)
	defer firstFlush.Stop()
	var flushTicker *time.Ticker
	flushTicks := firstFlush.C
	sendCtx, cancelSend := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSend()
	completion := make(chan metricSendResult, 2)
	hrQueue := make([]HeartRateEvent, 0, plan.BatchSize)
	stepsQueue := make([]StepEvent, 0, plan.BatchSize)
	var activeHR *authenticatedHeartRateBatch
	var activeSteps *authenticatedStepsBatch
	flushHR, flushSteps := false, false
	failedHR, failedSteps := false, false
	draining := false
	var drainTimer *time.Timer
	var drainTimeout <-chan time.Time
	var stopErr error
	ctxDone := ctx.Done()
	var hrUnknown, stepsUnknown int
	var hrAccepted, hrDuplicates, hrRejected, stepsAccepted, stepsDuplicates, stepsRejected int
	startDrain := func(err error) {
		if !draining {
			draining = true
			drainTimer = time.NewTimer(shutdownBudget)
			drainTimeout = drainTimer.C
			measurementTicker.Stop()
			if flushTicker != nil {
				flushTicker.Stop()
			}
			firstFlush.Stop()
			ctxDone = nil
			flushTicks = nil
		}
		if err != nil && stopErr == nil {
			stopErr = err
		}
	}
	finish := func(err error) error {
		if drainTimer != nil {
			drainTimer.Stop()
		}
		status := "completed"
		if err != nil || len(hrQueue) > 0 || len(stepsQueue) > 0 || hrUnknown > 0 || stepsUnknown > 0 || activeHR != nil || activeSteps != nil {
			status = "failed"
		}
		logger.Printf("producer_summary status=%s run_id=%s metrics=%s heart_rate_accepted=%d heart_rate_duplicates=%d heart_rate_rejected=%d steps_accepted=%d steps_duplicates=%d steps_rejected=%d heart_rate_queued_unsent=%d steps_queued_unsent=%d heart_rate_inflight_unconfirmed=%d steps_inflight_unconfirmed=%d", status, vuRunID, metricNames(plan.Metrics), hrAccepted, hrDuplicates, hrRejected, stepsAccepted, stepsDuplicates, stepsRejected, len(hrQueue), len(stepsQueue), hrUnknown+batchCountHR(activeHR), stepsUnknown+batchCountSteps(activeSteps))
		if len(hrQueue)+len(stepsQueue)+hrUnknown+stepsUnknown+batchCountHR(activeHR)+batchCountSteps(activeSteps) > 0 {
			outcomeErr := fmt.Errorf("account %d shutdown left queued_unsent=%d inflight_unconfirmed=%d", account.Index, len(hrQueue)+len(stepsQueue), hrUnknown+stepsUnknown+batchCountHR(activeHR)+batchCountSteps(activeSteps))
			if err != nil {
				return errors.Join(err, outcomeErr)
			}
			return outcomeErr
		}
		return err
	}
	launchHeart := func() {
		if activeHR != nil || len(hrQueue) == 0 || (!draining && !flushHR) || failedHR {
			return
		}
		events, rest := takeHeartRateChunk(hrQueue, plan.BatchSize)
		batch := authenticatedHeartRateBatch{DeviceID: deviceID, RunID: vuRunID, Events: events}
		hrQueue = rest
		activeHR = &batch
		if len(hrQueue) == 0 {
			flushHR = false
		}
		go func() {
			access, err := token.accessToken(sendCtx)
			var response authenticatedHeartRateResponse
			var status int
			if err == nil {
				response, status, err = heartClient.sendBatch(sendCtx, access, batch)
			}
			completion <- metricSendResult{metric: MetricHeartRate, hrBatch: batch, hrResult: response, status: status, err: err}
		}()
	}
	launchSteps := func() {
		if activeSteps != nil || len(stepsQueue) == 0 || (!draining && !flushSteps) || failedSteps {
			return
		}
		events, rest := takeStepsChunk(stepsQueue, plan.BatchSize)
		batch := authenticatedStepsBatch{DeviceID: stepsID, RunID: vuRunID, Events: events}
		stepsQueue = rest
		activeSteps = &batch
		if len(stepsQueue) == 0 {
			flushSteps = false
		}
		go func() {
			access, err := token.accessToken(sendCtx)
			var response authenticatedStepsResponse
			var status int
			if err == nil {
				response, status, err = stepsClient.sendBatch(sendCtx, access, batch)
			}
			completion <- metricSendResult{metric: MetricSteps, stBatch: batch, stResult: response, status: status, err: err}
		}()
	}
	for {
		if ctx.Err() != nil {
			startDrain(nil)
		}
		select {
		case <-drainTimeout:
			cancelSend()
			return finish(errors.New("producer shutdown exceeded the 5s drain budget"))
		default:
		}
		launchHeart()
		launchSteps()
		if draining && activeHR == nil && activeSteps == nil {
			return finish(stopErr)
		}
		select {
		case <-ctxDone:
			startDrain(nil)
		case <-flushTicks:
			if !draining {
				flushHR = plan.Metrics&MetricHeartRate != 0
				flushSteps = plan.Metrics&MetricSteps != 0
				if flushTicker == nil {
					flushTicker = time.NewTicker(plan.BatchInterval)
					flushTicks = flushTicker.C
				}
			}
		case tick := <-measurementTicker.C:
			if draining {
				continue
			}
			occurredAt := tick.UTC().Truncate(time.Millisecond)
			if heartGenerator != nil {
				if len(hrQueue)+batchCountHR(activeHR) >= queueLimit {
					startDrain(errors.New("Heart Rate event queue reached the 1000-event limit"))
					continue
				}
				phase := heartGenerator.activityPhase(occurredAt)
				hrQueue = append(hrQueue, heartGenerator.nextHeartRateEvent(occurredAt, phase))
			}
			if stepsGenerator != nil {
				if len(stepsQueue)+batchCountSteps(activeSteps) >= queueLimit {
					startDrain(errors.New("Steps event queue reached the 1000-event limit"))
					continue
				}
				if event, ok := stepsGenerator.Next(occurredAt); ok {
					stepsQueue = append(stepsQueue, event)
				}
			}
		case result := <-completion:
			if result.metric == MetricHeartRate {
				if result.err != nil {
					hrUnknown += len(result.hrBatch.Events)
					activeHR = nil
					failedHR = true
					startDrain(fmt.Errorf("Heart Rate batch failed after bounded retries: %w", result.err))
					logger.Printf("heart_rate_batch_failed run_id=%s submitted=%d http_status=%d error=%q inflight_unconfirmed=%d", vuRunID, len(result.hrBatch.Events), result.status, result.err.Error(), len(result.hrBatch.Events))
					continue
				}
				hrAccepted += result.hrResult.Accepted
				hrDuplicates += result.hrResult.Duplicates
				hrRejected += result.hrResult.Rejected
				logger.Printf("heart_rate_batch run_id=%s received=%d accepted=%d duplicates=%d rejected=%d", vuRunID, result.hrResult.Received, result.hrResult.Accepted, result.hrResult.Duplicates, result.hrResult.Rejected)
				for _, item := range result.hrResult.Results {
					if item.Status == "REJECTED" {
						code := ""
						if item.Error != nil {
							code = item.Error.Code
						}
						logger.Printf("heart_rate_rejected index=%d event_id=%s error_code=%s", item.Index, item.EventID, code)
					}
				}
				activeHR = nil
			} else {
				if result.err != nil {
					stepsUnknown += len(result.stBatch.Events)
					activeSteps = nil
					failedSteps = true
					startDrain(fmt.Errorf("Steps batch failed after bounded retries: %w", result.err))
					logger.Printf("steps_batch_failed run_id=%s submitted=%d http_status=%d error=%q inflight_unconfirmed=%d", vuRunID, len(result.stBatch.Events), result.status, result.err.Error(), len(result.stBatch.Events))
					continue
				}
				stepsAccepted += result.stResult.Accepted
				stepsDuplicates += result.stResult.Duplicates
				stepsRejected += result.stResult.Rejected
				logger.Printf("steps_batch run_id=%s received=%d accepted=%d duplicates=%d rejected=%d", vuRunID, result.stResult.Received, result.stResult.Accepted, result.stResult.Duplicates, result.stResult.Rejected)
				for _, item := range result.stResult.Results {
					if item.Status == "REJECTED" {
						code := ""
						if item.Error != nil {
							code = item.Error.Code
						}
						logger.Printf("steps_rejected index=%d event_id=%s error_code=%s", item.Index, item.EventID, code)
					}
				}
				activeSteps = nil
			}
		}
	}
}

func metricNames(metrics MetricSelection) string {
	if metrics == MetricBoth {
		return "heart-rate,steps"
	}
	if metrics == MetricSteps {
		return "steps"
	}
	return "heart-rate"
}

func spreadDuration(total time.Duration, index, count int) time.Duration {
	if count <= 0 {
		return 0
	}
	return total/time.Duration(count)*time.Duration(index) + (total%time.Duration(count))*time.Duration(index)/time.Duration(count)
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func batchCountHR(batch *authenticatedHeartRateBatch) int {
	if batch == nil {
		return 0
	}
	return len(batch.Events)
}

func takeHeartRateChunk(queue []HeartRateEvent, limit int) ([]HeartRateEvent, []HeartRateEvent) {
	n := min(limit, len(queue))
	return append([]HeartRateEvent(nil), queue[:n]...), queue[n:]
}

func batchCountSteps(batch *authenticatedStepsBatch) int {
	if batch == nil {
		return 0
	}
	return len(batch.Events)
}

func takeStepsChunk(queue []StepEvent, limit int) ([]StepEvent, []StepEvent) {
	n := min(limit, len(queue))
	return append([]StepEvent(nil), queue[:n]...), queue[n:]
}

type identityWriter struct {
	logger *log.Logger
	prefix string
}

func (w *identityWriter) Write(p []byte) (int, error) {
	w.logger.Printf("%s%s", w.prefix, strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}
