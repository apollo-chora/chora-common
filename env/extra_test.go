// Package env — supplementary LoadStruct edge tests.
package env_test

import (
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/env"
)

type taggedConfig struct {
	DBURL    string `env:"TEST_CHORA_DB_URL,required"`
	Port     string `env:"TEST_CHORA_PORT,default=8080"`
	internal string                // untagged + unexported — must be skipped
}

func TestLoadStruct_SkipsUnexportedFields(t *testing.T) {
	t.Setenv("TEST_CHORA_DB_URL", "postgres://x")
	t.Setenv("TEST_CHORA_PORT", "7070")
	var cfg taggedConfig
	if err := env.LoadStruct(&cfg); err != nil {
		t.Fatalf("LoadStruct: %v", err)
	}
	if cfg.DBURL != "postgres://x" {
		t.Errorf("DBURL = %q", cfg.DBURL)
	}
	if cfg.Port != "7070" {
		t.Errorf("Port = %q", cfg.Port)
	}
}

type nonStringConfig struct {
	Replicas int `env:"TEST_CHORA_REPLICAS"`
}

func TestLoadStruct_RejectsNonStringField(t *testing.T) {
	t.Setenv("TEST_CHORA_REPLICAS", "3")
	var cfg nonStringConfig
	err := env.LoadStruct(&cfg)
	if err == nil {
		t.Fatal("expected error for non-string tagged field")
	}
	if !strings.Contains(err.Error(), "must be a settable string") {
		t.Errorf("unexpected error: %v", err)
	}
}
