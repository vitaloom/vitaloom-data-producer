package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	logger := log.New(os.Stdout, "[vitaloom-producer] ", log.LstdFlags|log.LUTC)
	if len(args) > 0 && args[0] == "provision" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runProvision(ctx, args[1:], getenv, logger)
	}
	if len(args) > 0 && args[0] == "heart-rate" {
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
	if len(args) > 0 && args[0] == "step" {
		options, cfg, password, err := parseStepsCommand(args[1:], getenv)
		if err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runStepsScheduled(ctx, cfg, password, options, logger)
	}
	options, cfg, password, err := parseCombinedCommand(args, getenv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runAuthenticatedCombined(ctx, cfg, password, options, logger)
}
