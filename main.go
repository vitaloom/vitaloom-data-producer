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
	plan, password, err := ParseRunPlan(args, getenv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runPlan(ctx, plan, password, logger)
}
