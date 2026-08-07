package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Subscriptions.Refresh != 6*time.Hour {
		t.Errorf("default refresh = %s, want 6h", cfg.Subscriptions.Refresh)
	}
	if cfg.Health.Workers != 50 {
		t.Errorf("default workers = %d, want 50", cfg.Health.Workers)
	}
	if !cfg.Health.Enabled || !cfg.Speed.Enabled || !cfg.API.Dashboard {
		t.Error("default booleans must be enabled")
	}
	if cfg.Log.Level != "info" {
		t.Errorf("default log level = %q, want info", cfg.Log.Level)
	}
	if cfg.Scoring.SwitchDelta != 0.10 {
		t.Errorf("default switch_delta = %f, want 0.10", cfg.Scoring.SwitchDelta)
	}
	if cfg.Selector.Mode != "auto" {
		t.Errorf("default selector mode = %q, want auto", cfg.Selector.Mode)
	}
}

func TestLoadYAMLOverridesDefaults(t *testing.T) {
	path := writeConfig(t, `
log:
  level: debug
health:
  workers: 8
scoring:
  switch_delta: 0.25
inbound:
  listeners:
    - protocol: vless-reality
      enabled: true
      port: 443
      uuid: "11111111-1111-1111-1111-111111111111"
      server_names: [www.example.com]
      dest: "www.example.com:443"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("level = %q, want debug", cfg.Log.Level)
	}
	if cfg.Health.Workers != 8 {
		t.Errorf("workers = %d, want 8", cfg.Health.Workers)
	}
	if cfg.Scoring.SwitchDelta != 0.25 {
		t.Errorf("switch_delta = %f, want 0.25", cfg.Scoring.SwitchDelta)
	}
	if len(cfg.Inbound.Listeners) != 1 {
		t.Fatalf("listeners = %d, want 1", len(cfg.Inbound.Listeners))
	}
}

func TestEnvOverridesYAML(t *testing.T) {
	t.Setenv("SG_HEALTH_WORKERS", "12")
	t.Setenv("SG_SUBSCRIPTIONS_REFRESH_INTERVAL", "90m")
	t.Setenv("SG_API_DASHBOARD", "false")
	t.Setenv("SG_SELECTOR_MODE", "manual")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Health.Workers != 12 {
		t.Errorf("workers = %d, want 12", cfg.Health.Workers)
	}
	if cfg.Subscriptions.Refresh != 90*time.Minute {
		t.Errorf("refresh = %s, want 90m", cfg.Subscriptions.Refresh)
	}
	if cfg.API.Dashboard {
		t.Error("dashboard must be disabled via env")
	}
	if cfg.Selector.Mode != "manual" {
		t.Errorf("mode = %q, want manual", cfg.Selector.Mode)
	}
}

func TestEnvInvalidValueFails(t *testing.T) {
	t.Setenv("SG_HEALTH_WORKERS", "abc")
	_, err := Load("")
	if err == nil {
		t.Fatal("expected error for invalid env value")
	}
}

func TestValidationAggregatesErrors(t *testing.T) {
	path := writeConfig(t, `
log:
  level: bogus
scoring:
  switch_delta: 2
inbound:
  listeners:
    - protocol: nope
      port: 0
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	ev, ok := err.(*Errors)
	if !ok {
		t.Fatalf("expected *Errors, got %T", err)
	}
	if len(ev.Findings()) < 3 {
		t.Errorf("expected >=3 findings, got %d: %v", len(ev.Findings()), ev.Findings())
	}
	msg := err.Error()
	for _, want := range []string{"log.level", "switch_delta", "protocol"} {
		if !strings.Contains(msg, want) {
			t.Errorf("validation message missing %q: %s", want, msg)
		}
	}
}

func TestValidationAcceptsRealisticConfig(t *testing.T) {
	path := writeConfig(t, `
api:
  listen: "0.0.0.0:8080"
core:
  singbox_path: /usr/local/bin/sing-box
subscriptions:
  file: subscriptions.txt
  refresh_interval: 4h
health:
  workers: 100
  max_probe_mb_per_day: 50
speed:
  download_size_bytes: 2097152
inbound:
  listeners:
    - protocol: vless-reality
      enabled: true
      listen: "0.0.0.0"
      port: 443
      uuid: "11111111-1111-1111-1111-111111111111"
      flow: xtls-rprx-vision
      server_names: [www.microsoft.com, www.cloudflare.com]
      dest: "www.microsoft.com:443"
      short_id_len: 8
    - protocol: shadowsocks
      enabled: true
      port: 8388
      password: strong-pass
      method: 2022-blake3-aes-128-gcm
alerting:
  telegram:
    enabled: true
    bot_token: "123:abc"
    chat_id: "42"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("realistic config rejected: %v", err)
	}
	if len(cfg.Inbound.Listeners) != 2 {
		t.Errorf("listeners = %d, want 2", len(cfg.Inbound.Listeners))
	}
}

func TestLoadMissingFileFails(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestInvalidUUIDRejected(t *testing.T) {
	path := writeConfig(t, `
inbound:
  listeners:
    - protocol: vless
      port: 8443
      uuid: not-a-uuid
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid uuid")
	}
}

func TestTelegramRequiresToken(t *testing.T) {
	path := writeConfig(t, `
alerting:
  telegram:
    enabled: true
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error: telegram enabled without token")
	}
}
