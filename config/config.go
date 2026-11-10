package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

func Load[T any]() (T, error) {
	cfg, err := env.ParseAs[T]()
	if err != nil {
		var zero T
		return zero, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

func LoadFrom[T any](environ map[string]string) (T, error) {
	cfg, err := env.ParseAsWithOptions[T](env.Options{Environment: environ})
	if err != nil {
		var zero T
		return zero, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}
