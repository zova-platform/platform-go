package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/zova-platform/platform-go/config"
)

type appConfig struct {
	Name    string        `env:"APP_NAME,required"`
	Port    int           `env:"APP_PORT" envDefault:"8080"`
	Timeout time.Duration `env:"APP_TIMEOUT" envDefault:"5s"`
}

func TestLoadFromDefaults(t *testing.T) {
	cfg, err := config.LoadFrom[appConfig](map[string]string{"APP_NAME": "trip"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Name != "trip" || cfg.Port != 8080 || cfg.Timeout != 5*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadFromOverrides(t *testing.T) {
	cfg, err := config.LoadFrom[appConfig](map[string]string{
		"APP_NAME":    "trip",
		"APP_PORT":    "9000",
		"APP_TIMEOUT": "2m",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 9000 || cfg.Timeout != 2*time.Minute {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadFromMissingRequired(t *testing.T) {
	_, err := config.LoadFrom[appConfig](map[string]string{})
	if err == nil || !strings.Contains(err.Error(), "APP_NAME") {
		t.Fatalf("want error naming APP_NAME, got %v", err)
	}
}

func TestLoadFromInvalidValue(t *testing.T) {
	_, err := config.LoadFrom[appConfig](map[string]string{"APP_NAME": "x", "APP_PORT": "abc"})
	if err == nil {
		t.Fatal("want error for non-numeric port")
	}
}

func TestLoadReadsProcessEnvironment(t *testing.T) {
	t.Setenv("APP_NAME", "pricing")
	cfg, err := config.Load[appConfig]()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Name != "pricing" {
		t.Fatalf("got %q", cfg.Name)
	}
}
