package main

import "time"

const (
	maxBatchSize         = 500
	maxResponseBodyBytes = 64 * 1024
)

var hoChiMinhLocation = time.FixedZone("Asia/Ho_Chi_Minh", 7*60*60)

type Config struct {
	APIBaseURL               string
	BatchSize                int
	Interval                 time.Duration // compatibility name for the shared sync interval
	HeartRateInterval        time.Duration
	StepsAggregationInterval time.Duration
	Duration                 time.Duration
	Timeout                  time.Duration
	MaxRetries               int
	RetryDelay               time.Duration
}

const (
	defaultHeartRateInterval        = 300 * time.Second
	defaultStepsAggregationInterval = 60 * time.Second
	defaultSyncInterval             = 60 * time.Second
)
