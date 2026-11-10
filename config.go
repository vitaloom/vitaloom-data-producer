package main

import "time"

const (
	maxBatchSize         = 500
	maxResponseBodyBytes = 64 * 1024
)

type Config struct {
	APIBaseURL string
	BatchSize  int
	Interval   time.Duration
	Duration   time.Duration
	Timeout    time.Duration
	MaxRetries int
	RetryDelay time.Duration
}
