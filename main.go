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
	if err := run(os.Args[1:]); err != nil { log.Printf("ERROR: %v", err); os.Exit(1) }
}
func run(args []string) error {
	cfg, err := ParseConfig(args, os.Getenv)
	if err != nil { if errors.Is(err, flag.ErrHelp) { return nil }; return err }
	logger := log.New(os.Stdout, "[vitaloom-producer] ", log.LstdFlags|log.LUTC)
	runID, err := NewRunID(time.Now())
	if err != nil { return err }
	generator := NewEventGenerator(runID, time.Now(), nil)
	client := NewAPIClient(cfg, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stats, runErr := runProducer(ctx, cfg, client, logger, generator)
	logSummary(logger, stats, runErr)
	return runErr
}
