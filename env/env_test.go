package env_test

import (
	"os"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/env"
)

func TestMustGet_PanicsWhenMissing(t *testing.T) {
	const key = "CHORA_TEST_MISSING_KEY_XYZ"
	_ = os.Unsetenv(key)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on missing required env var")
		}
	}()
	env.MustGet(key)
}

func TestMustGet_ReturnsValueWhenPresent(t *testing.T) {
	const key = "CHORA_TEST_PRESENT_KEY"
	t.Setenv(key, "expected-value")
	got := env.MustGet(key)
	if got != "expected-value" {
		t.Errorf("MustGet=%q want %q", got, "expected-value")
	}
}

func TestMustGet_PanicsWhenEmpty(t *testing.T) {
	const key = "CHORA_TEST_EMPTY_KEY"
	t.Setenv(key, "")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty env var (treated as missing)")
		}
	}()
	env.MustGet(key)
}

func TestGetOrDefault(t *testing.T) {
	cases := []struct {
		name     string
		key      string
		setVal   string
		setEnv   bool
		fallback string
		want     string
	}{
		{"missing returns default", "CHORA_TEST_MISS_1", "", false, "fallback", "fallback"},
		{"empty returns default", "CHORA_TEST_EMPTY_1", "", true, "fallback", "fallback"},
		{"present returns value", "CHORA_TEST_PRESENT_1", "actual", true, "fallback", "actual"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_ = os.Unsetenv(c.key)
			if c.setEnv {
				t.Setenv(c.key, c.setVal)
			}
			got := env.GetOrDefault(c.key, c.fallback)
			if got != c.want {
				t.Errorf("GetOrDefault(%q,%q)=%q want %q", c.key, c.fallback, got, c.want)
			}
		})
	}
}

type sampleConfig struct {
	ServiceName string `env:"CHORA_SAMPLE_SERVICE_NAME,required"`
	Port        string `env:"CHORA_SAMPLE_PORT,default=8080"`
	Optional    string `env:"CHORA_SAMPLE_OPTIONAL"`
	Untagged    string
}

func TestLoadStruct_PopulatesFields(t *testing.T) {
	t.Setenv("CHORA_SAMPLE_SERVICE_NAME", "chora-test")
	t.Setenv("CHORA_SAMPLE_PORT", "9090")
	t.Setenv("CHORA_SAMPLE_OPTIONAL", "value")

	var cfg sampleConfig
	if err := env.LoadStruct(&cfg); err != nil {
		t.Fatalf("LoadStruct: %v", err)
	}
	if cfg.ServiceName != "chora-test" {
		t.Errorf("ServiceName=%q want %q", cfg.ServiceName, "chora-test")
	}
	if cfg.Port != "9090" {
		t.Errorf("Port=%q want %q", cfg.Port, "9090")
	}
	if cfg.Optional != "value" {
		t.Errorf("Optional=%q want %q", cfg.Optional, "value")
	}
}

func TestLoadStruct_AppliesDefaults(t *testing.T) {
	t.Setenv("CHORA_SAMPLE_SERVICE_NAME", "chora-default")
	_ = os.Unsetenv("CHORA_SAMPLE_PORT")
	_ = os.Unsetenv("CHORA_SAMPLE_OPTIONAL")

	var cfg sampleConfig
	if err := env.LoadStruct(&cfg); err != nil {
		t.Fatalf("LoadStruct: %v", err)
	}
	if cfg.Port != "8080" {
		t.Errorf("Port=%q want default %q", cfg.Port, "8080")
	}
	if cfg.Optional != "" {
		t.Errorf("Optional=%q want empty", cfg.Optional)
	}
}

func TestLoadStruct_MissingRequiredErrors(t *testing.T) {
	_ = os.Unsetenv("CHORA_SAMPLE_SERVICE_NAME")
	_ = os.Unsetenv("CHORA_SAMPLE_PORT")

	var cfg sampleConfig
	err := env.LoadStruct(&cfg)
	if err == nil {
		t.Fatal("expected error for missing required field")
	}
}

func TestLoadStruct_RejectsNonPointer(t *testing.T) {
	var cfg sampleConfig
	err := env.LoadStruct(cfg)
	if err == nil {
		t.Fatal("expected error for non-pointer arg")
	}
}

func TestLoadStruct_RejectsNonStruct(t *testing.T) {
	var s string
	err := env.LoadStruct(&s)
	if err == nil {
		t.Fatal("expected error for non-struct pointer")
	}
}
