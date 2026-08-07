package config

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

// Errors aggregates every configuration problem found during validation.
// FR-023 requires descriptive errors; FR-003 forbids partial startup,
// so callers must treat a non-nil Errors as fatal.
type Errors struct {
	list []string
}

func (e *Errors) addf(format string, args ...any) {
	e.list = append(e.list, fmt.Sprintf(format, args...))
}

// Error implements the error interface, joining all findings.
func (e *Errors) Error() string {
	if len(e.list) == 0 {
		return "no configuration errors"
	}
	return "invalid configuration: " + strings.Join(e.list, "; ")
}

// Findings returns the individual validation messages.
func (e *Errors) Findings() []string { return e.list }

var (
	uuidRe         = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	validProtocols = map[string]bool{
		"vless-reality": true, "vless": true, "vmess": true, "trojan": true,
		"shadowsocks": true, "hysteria2": true, "tuic": true,
	}
)

// Validate checks the whole configuration and returns nil when valid.
func (c *Config) Validate() *Errors {
	errs := &Errors{}

	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		errs.addf("log.level: must be one of debug|info|warn|error, got %q", c.Log.Level)
	}

	if err := validateHostPort("api.listen", c.API.Listen); err != "" {
		errs.addf("%s", err)
	}

	if strings.TrimSpace(c.Core.SingboxPath) == "" {
		errs.addf("core.singbox_path: must not be empty")
	}
	if err := validateHostPort("core.api_listen", c.Core.APIListen); err != "" {
		errs.addf("%s", err)
	}
	if c.Core.RestartLimit < 0 {
		errs.addf("core.restart_limit: must be >= 0, got %d", c.Core.RestartLimit)
	}

	if strings.TrimSpace(c.Subscriptions.File) == "" {
		errs.addf("subscriptions.file: must not be empty")
	}
	if c.Subscriptions.Refresh < time.Minute {
		errs.addf("subscriptions.refresh_interval: must be >= 1m, got %s", c.Subscriptions.Refresh)
	}
	if c.Subscriptions.RequestTimeout < time.Second || c.Subscriptions.RequestTimeout > time.Minute {
		errs.addf("subscriptions.request_timeout: must be within [1s,60s], got %s", c.Subscriptions.RequestTimeout)
	}
	if c.Subscriptions.MaxBytes < 1<<10 || c.Subscriptions.MaxBytes > 256<<20 {
		errs.addf("subscriptions.max_bytes: must be within [1KiB,256MiB], got %d", c.Subscriptions.MaxBytes)
	}
	if c.Subscriptions.BackoffBase <= 0 || c.Subscriptions.BackoffMax < c.Subscriptions.BackoffBase {
		errs.addf("subscriptions backoff: base must be > 0 and max >= base")
	}
	for i, d := range c.Subscriptions.Defs {
		if strings.TrimSpace(d.URL) == "" {
			errs.addf("subscriptions.defs[%d].url: must not be empty", i)
		}
	}

	c.validateInbounds(errs)
	c.validateHealth(errs)
	c.validateSpeed(errs)
	c.validateScoring(errs)

	if c.Selector.Mode != "auto" && c.Selector.Mode != "manual" {
		errs.addf("selector.mode: must be auto|manual, got %q", c.Selector.Mode)
	}

	if strings.TrimSpace(c.Storage.Path) == "" {
		errs.addf("storage.path: must not be empty")
	}

	if c.Alerting.Telegram.Enabled {
		if strings.TrimSpace(c.Alerting.Telegram.BotToken) == "" {
			errs.addf("alerting.telegram.bot_token: required when telegram enabled")
		}
		if strings.TrimSpace(c.Alerting.Telegram.ChatID) == "" {
			errs.addf("alerting.telegram.chat_id: required when telegram enabled")
		}
	}
	for i, w := range c.Alerting.Webhooks {
		if strings.TrimSpace(w.URL) == "" {
			errs.addf("alerting.webhooks[%d].url: must not be empty", i)
		}
	}

	if len(errs.list) == 0 {
		return nil
	}
	return errs
}

func (c *Config) validateInbounds(errs *Errors) {
	for i, l := range c.Inbound.Listeners {
		prefix := fmt.Sprintf("inbound.listeners[%d]", i)
		if !validProtocols[l.Protocol] {
			errs.addf("%s.protocol: unsupported %q", prefix, l.Protocol)
			continue
		}
		if l.Port < 1 || l.Port > 65535 {
			errs.addf("%s.port: must be within [1,65535], got %d", prefix, l.Port)
		}
		if l.Listen != "" {
			if ip := net.ParseIP(l.Listen); ip == nil && !validHostname(l.Listen) {
				errs.addf("%s.listen: invalid host %q", prefix, l.Listen)
			}
		}
		if l.UUID != "" && !uuidRe.MatchString(l.UUID) {
			errs.addf("%s.uuid: invalid UUID %q", prefix, l.UUID)
		}
		if l.Protocol == "vless-reality" || l.Protocol == "vless" {
			if l.UUID == "" {
				errs.addf("%s.uuid: required for %s", prefix, l.Protocol)
			}
			if l.Protocol == "vless-reality" {
				if len(l.ServerNames) == 0 {
					errs.addf("%s.server_names: required for vless-reality", prefix)
				}
				if l.Dest == "" {
					errs.addf("%s.dest: required for vless-reality", prefix)
				}
				if l.ShortIDLength < 1 || l.ShortIDLength > 16 {
					errs.addf("%s.short_id_len: must be within [1,16], got %d", prefix, l.ShortIDLength)
				}
			}
		}
		if l.Protocol == "shadowsocks" && l.Method != "" && !validSSMethod(l.Method) {
			errs.addf("%s.method: unsupported shadowsocks cipher %q", prefix, l.Method)
		}
		if (l.Protocol == "vmess" || l.Protocol == "trojan" || l.Protocol == "hysteria2" || l.Protocol == "tuic" || l.Protocol == "shadowsocks") && l.Password == "" && len(l.Users) == 0 {
			errs.addf("%s.password or %s.users: required for %s", prefix, prefix, l.Protocol)
		}
		if l.TLS.CertFile != "" && l.TLS.KeyFile == "" || l.TLS.KeyFile != "" && l.TLS.CertFile == "" {
			errs.addf("%s.tls: cert_file and key_file must be provided together", prefix)
		}
	}
}

func (c *Config) validateHealth(errs *Errors) {
	if c.Health.Workers < 1 || c.Health.Workers > 1024 {
		errs.addf("health.workers: must be within [1,1024], got %d", c.Health.Workers)
	}
	if c.Health.ProbeTimeout < 100*time.Millisecond || c.Health.ProbeTimeout > time.Minute {
		errs.addf("health.probe_timeout: must be within [100ms,60s], got %s", c.Health.ProbeTimeout)
	}
	if c.Health.CandidateWindow < time.Second {
		errs.addf("health.candidate_window: must be >= 1s, got %s", c.Health.CandidateWindow)
	}
	if c.Health.FleetInterval < c.Health.CandidateWindow {
		errs.addf("health.fleet_interval: must be >= candidate_window")
	}
	if c.Health.MaxProbeBytes < 1 || c.Health.MaxProbeBytes > 4096 {
		errs.addf("health.max_probe_mb_per_day: must be within [1MiB,4096MiB], got %d MiB", c.Health.MaxProbeBytes)
	}
	if c.Health.CandidatePool < 10 {
		errs.addf("health.candidate_pool_size: must be >= 10, got %d", c.Health.CandidatePool)
	}
	if c.Health.MinInterval < time.Second {
		errs.addf("health.min_interval: must be >= 1s, got %s", c.Health.MinInterval)
	}
}

func (c *Config) validateSpeed(errs *Errors) {
	if c.Speed.Interval < 10*time.Minute {
		errs.addf("speed.interval: must be >= 10m (speed tests must be much rarer than health checks), got %s", c.Speed.Interval)
	}
	if c.Speed.DownloadSize < 1<<10 || c.Speed.DownloadSize > 50<<20 {
		errs.addf("speed.download_size_bytes: must be within [1KiB,50MiB], got %d", c.Speed.DownloadSize)
	}
	if c.Speed.Concurrency < 1 || c.Speed.Concurrency > 50 {
		errs.addf("speed.concurrency: must be within [1,50], got %d", c.Speed.Concurrency)
	}
	if c.Speed.MaxPerDay < c.Speed.DownloadSize {
		errs.addf("speed.max_bytes_per_day: must be >= download_size_bytes")
	}
}

func (c *Config) validateScoring(errs *Errors) {
	w := []float64{c.Scoring.LatencyWeight, c.Scoring.StabilityWeight, c.Scoring.ThroughputWeight, c.Scoring.LossWeight}
	for _, x := range w {
		if x < 0 {
			errs.addf("scoring weights: must all be >= 0")
			break
		}
	}
	sum := w[0] + w[1] + w[2] + w[3]
	if sum <= 0 {
		errs.addf("scoring weights: at least one weight must be > 0")
	}
	if c.Scoring.EWMASamples < 2 {
		errs.addf("scoring.ewma_samples: must be >= 2, got %d", c.Scoring.EWMASamples)
	}
	if c.Scoring.MinSamples < 1 {
		errs.addf("scoring.min_samples: must be >= 1, got %d", c.Scoring.MinSamples)
	}
	if c.Scoring.SwitchDelta <= 0 || c.Scoring.SwitchDelta > 1 {
		errs.addf("scoring.switch_delta: must be within (0,1], got %f", c.Scoring.SwitchDelta)
	}
	if c.Scoring.MinTicks < 1 {
		errs.addf("scoring.min_ticks: must be >= 1, got %d", c.Scoring.MinTicks)
	}
	if c.Scoring.DwellTime < time.Second {
		errs.addf("scoring.dwell_time: must be >= 1s, got %s", c.Scoring.DwellTime)
	}
	if c.Scoring.FailoverFailures < 1 {
		errs.addf("scoring.failover_failures: must be >= 1, got %d", c.Scoring.FailoverFailures)
	}
	if c.Scoring.HardLatency < 100*time.Millisecond {
		errs.addf("scoring.hard_latency_ms: must be >= 100ms, got %s", c.Scoring.HardLatency)
	}
}

func validateHostPort(field, addr string) string {
	if strings.TrimSpace(addr) == "" {
		return fmt.Sprintf("%s: must not be empty", field)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Sprintf("%s: invalid host:port %q", field, addr)
	}
	if host == "" || port == "" {
		return fmt.Sprintf("%s: invalid host:port %q", field, addr)
	}
	return ""
}

func validHostname(h string) bool {
	if len(h) == 0 || len(h) > 253 {
		return false
	}
	for _, part := range strings.Split(h, ".") {
		if len(part) == 0 || len(part) > 63 {
			return false
		}
		for _, r := range part {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	return true
}

func validSSMethod(m string) bool {
	switch m {
	case "2022-blake3-aes-128-gcm", "2022-blake3-aes-256-gcm", "2022-blake3-chacha20-poly1305",
		"aes-256-gcm", "aes-128-gcm", "chacha20-ietf-poly1305", "plain", "none":
		return true
	}
	return false
}
