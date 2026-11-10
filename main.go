package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	dotenv, err := LoadDotEnv(".env")
	if err != nil {
		return err
	}
	getenv := environmentLookup(dotenv)
	if len(args) > 0 && args[0] == "provision" {
		logger := log.New(os.Stdout, "[vitaloom-producer] ", log.LstdFlags|log.LUTC)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runProvision(ctx, args[1:], getenv, logger)
	}
	if len(args) > 0 && args[0] == "heart-rate" {
		logger := log.New(os.Stdout, "[vitaloom-producer] ", log.LstdFlags|log.LUTC)
		if hasVirtualUsersFlag(args[1:]) {
			options, cfg, password, err := parseHeartRateMultiUserCommand(args[1:], getenv)
			if err != nil {
				if errors.Is(err, flag.ErrHelp) {
					return nil
				}
				return err
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return runHeartRateMultiUser(ctx, cfg, password, options, logger)
		}
		cfg, password, accountIndex, measurementInterval, batchInterval, duration, scheduled, err := parseHeartRateCommand(args[1:], getenv)
		if err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if scheduled {
			return runHeartRateScheduled(ctx, cfg, password, accountIndex, measurementInterval, batchInterval, duration, logger)
		}
		return runHeartRateOnce(ctx, cfg, password, accountIndex, logger)
	}
	cfg, err := ParseConfig(args, getenv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	logger := log.New(os.Stdout, "[vitaloom-producer] ", log.LstdFlags|log.LUTC)
	runID, err := NewRunID(time.Now())
	if err != nil {
		return err
	}
	generator := NewEventGenerator(runID, time.Now(), nil)
	client := NewAPIClient(cfg, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stats, runErr := runProducer(ctx, cfg, client, logger, generator)
	logSummary(logger, stats, runErr)
	return runErr
}
