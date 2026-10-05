// Package env loads + validates environment-sourced configuration.
//
// Per CLAUDE.md §6 ("No inline config") + memory feedback_no_inline_config:
// all URLs, secrets, third-party adapter configs, and stub endpoints MUST be
// read from env vars (sourced from Terraform / a secret store). Source
// code MUST NOT contain inlined configuration.
//
// This package gives services three ergonomic helpers:
//
//   - MustGet(key)              — fail-fast read, panics if missing/empty.
//   - GetOrDefault(key, def)    — soft read with caller-provided fallback.
//   - LoadStruct(&cfg)          — populate a tagged struct from env in one call.
//
// Struct tag schema:
//
//	type Config struct {
//	    DBURL    string `env:"CHORA_DB_URL,required"`
//	    Port     string `env:"CHORA_PORT,default=8080"`
//	    Optional string `env:"CHORA_OPTIONAL"`
//	}
package env

import (
	"fmt"
	"os"
	"reflect"
	"strings"
)

// MustGet returns the value of the named env var, panicking if it is missing
// or empty. Use for required configuration where startup must fail loudly.
func MustGet(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("env: required variable %q is unset or empty", key))
	}
	return v
}

// GetOrDefault returns the env var if present and non-empty, otherwise the
// supplied fallback. Use for optional config with safe defaults.
func GetOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// LoadStruct populates the fields of target (which MUST be a pointer to a
// struct) from environment variables declared via `env:"…"` tags.
//
// Tag values: "KEY", "KEY,required", "KEY,default=foo".
// Untagged fields are skipped. Only string fields are supported in this
// minimal helper — services that need typed parsing wire it themselves.
func LoadStruct(target interface{}) error {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("env: LoadStruct expects non-nil pointer, got %T", target)
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("env: LoadStruct expects pointer to struct, got pointer to %s", rv.Kind())
	}

	t := rv.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("env")
		if tag == "" {
			continue
		}
		key, required, def := parseTag(tag)
		raw := os.Getenv(key)
		if raw == "" {
			if required {
				return fmt.Errorf("env: required variable %q is unset", key)
			}
			raw = def
		}
		fv := rv.Field(i)
		if fv.Kind() != reflect.String || !fv.CanSet() {
			return fmt.Errorf("env: field %q must be a settable string", field.Name)
		}
		fv.SetString(raw)
	}
	return nil
}

// parseTag splits "KEY,required" or "KEY,default=foo" into components.
func parseTag(tag string) (key string, required bool, def string) {
	parts := strings.Split(tag, ",")
	key = strings.TrimSpace(parts[0])
	for _, mod := range parts[1:] {
		mod = strings.TrimSpace(mod)
		switch {
		case mod == "required":
			required = true
		case strings.HasPrefix(mod, "default="):
			def = strings.TrimPrefix(mod, "default=")
		}
	}
	return key, required, def
}
