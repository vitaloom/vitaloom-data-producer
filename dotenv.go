package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type EnvLookup func(string) string

var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// LoadDotEnv reads simple NAME=value entries without changing the process environment.
func LoadDotEnv(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("read .env: %w", err)
	}
	defer file.Close()

	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if lineNumber == 1 {
			line = strings.TrimPrefix(line, "\uFEFF")
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		name = strings.TrimSpace(name)
		if !ok || !envNamePattern.MatchString(name) {
			return nil, fmt.Errorf(".env line %d: expected NAME=value", lineNumber)
		}
		values[name] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read .env: %w", err)
	}
	return values, nil
}

func environmentLookup(dotenv map[string]string) EnvLookup {
	return func(name string) string {
		if value, ok := os.LookupEnv(name); ok {
			return value
		}
		return dotenv[name]
	}
}
