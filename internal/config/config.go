// Package config defines the Smart Gateway configuration schema, defaults,
// environment overrides and validation.
//
// Rules enforced here:
//   - configuration is loaded from YAML (FR-020)
//   - environment variables override YAML values (FR-021)
//   - invalid configuration produces descriptive, aggregated errors (FR-023)
//   - every field documents its default, allowed range and validation rule (FR-024)
//   - the loaded value is immutable; no subsystem reads files directly (architecture §21)
package config

import (
	"time"
)

// MiB is a whole number of mebibytes (1024*1024 bytes).
// YAML and environment values carry the unit in their key name
// (e.g. max_probe_mb_per_day), so the stored number is in MiB.
type MiB int64

// Config is the complete gateway configuration.
type Config struct {
	SourcePath string `yaml:"-"` // path the YAML was loaded from; empty when defaults-only (reload metadata)

	Log           LogConfig           `yaml:"log"`
	API           APIConfig           `yaml:"api"`
	Core          CoreConfig          `yaml:"core"`
	Subscriptions SubscriptionsConfig `yaml:"subscriptions"`
	Inbound       InboundConfig       `yaml:"inbound"`
	Health        HealthConfig        `yaml:"health"`
	Speed         SpeedConfig         `yaml:"speed"`
	Scoring       ScoringConfig       `yaml:"scoring"`
	Selector      SelectorConfig      `yaml:"selector"`
	Storage       StorageConfig       `yaml:"storage"`
	Alerting      AlertingConfig      `yaml:"alerting"`
}

// LogConfig configures logging behaviour.
type LogConfig struct {
	Level string `yaml:"level"` // debug | info | warn | error (default: info)
	JSON  bool   `yaml:"json"`  // always true in practice; kept for parity (default: true)
}

// APIConfig configures the REST API and dashboard.
type APIConfig struct {
	Listen     string `yaml:"listen"`      // host:port for API+dashboard (default: 0.0.0.0:8080)
	Token      string `yaml:"token"`       // admin token; empty -> auto-generated on first start (default: "")
	PublicHost string `yaml:"public_host"` // public host/domain clients dial; empty -> derived from inbounds (default: "")
	Dashboard  bool   `yaml:"dashboard"`
}

// CoreConfig configures the sing-box data plane integration.
type CoreConfig struct {
	SingboxPath  string `yaml:"singbox_path"`  // path to the sing-box binary (default: /usr/local/bin/sing-box)
	APIListen    string `yaml:"api_listen"`    // Clash API listen address (default: 127.0.0.1:9090)
	APISecret    string `yaml:"api_secret"`    // Clash API secret; empty -> auto-generated (default: "")
	CacheFile    string `yaml:"cache_file"`    // persisting selection across restarts (default: data/core-cache.db)
	ConfigDir    string `yaml:"config_dir"`    // directory holding generated core configs (default: data/core)
	RestartLimit int    `yaml:"restart_limit"` // max auto-restarts per hour before alerting (default: 5)
}

// SubscriptionsConfig configures provider subscription handling.
type SubscriptionsConfig struct {
	File           string            `yaml:"file"`             // path to subscriptions.txt (default: subscriptions.txt)
	Refresh        time.Duration     `yaml:"refresh_interval"` // base interval; jitter applied (default: 6h)
	RequestTimeout time.Duration     `yaml:"request_timeout"`  // per-download timeout (default: 30s)
	MaxBytes       int64             `yaml:"max_bytes"`        // download size cap (default: 10MiB)
	UserAgent      string            `yaml:"user_agent"`       // HTTP user-agent (default: SmartGateway/1.0)
	VerifyTLS      bool              `yaml:"verify_tls"`       // TLS verification; disable only explicitly (default: true)
	BackoffBase    time.Duration     `yaml:"backoff_base"`     // exponential backoff base (default: 1m)
	BackoffMax     time.Duration     `yaml:"backoff_max"`      // backoff ceiling (default: 4h)
	RetryLimit     int               `yaml:"retry_limit"`      // max consecutive failures before alert (default: 5)
	Defs           []SubscriptionDef `yaml:"defs"`             // inline subscriptions with metadata (optional)
}

// SubscriptionDef is an inline subscription definition carrying FR-103 metadata.
type SubscriptionDef struct {
	Name        string            `yaml:"name"`
	Provider    string            `yaml:"provider"`
	Priority    int               `yaml:"priority"`
	Tags        []string          `yaml:"tags"`
	Enabled     *bool             `yaml:"enabled"`
	URL         string            `yaml:"url"`
	UpdateEvery *time.Duration    `yaml:"update_every"`
	Headers     map[string]string `yaml:"headers"`
	Timeout     *time.Duration    `yaml:"timeout"`
}

// InboundConfig defines the stable client-facing inbound listeners.
type InboundConfig struct {
	Listeners []ListenerConfig `yaml:"listeners"`
}

// ListenerConfig is one inbound listener.
type ListenerConfig struct {
	Protocol      string     `yaml:"protocol"` // vless-reality | vless | vmess | trojan | shadowsocks | hysteria2 | tuic
	Enabled       bool       `yaml:"enabled"`
	Listen        string     `yaml:"listen"` // host (default: 0.0.0.0)
	Port          int        `yaml:"port"`
	UUID          string     `yaml:"uuid"`         // for vless/vmess/trojan-style users; empty -> auto-generate
	Flow          string     `yaml:"flow"`         // for vless (default: xtls-rprx-vision)
	ServerNames   []string   `yaml:"server_names"` // reality SNI allow-list
	Dest          string     `yaml:"dest"`         // reality fallback target host:port
	ShortIDLength int        `yaml:"short_id_len"`
	Password      string     `yaml:"password"`
	Method        string     `yaml:"method"` // shadowsocks cipher
	Users         []UserSpec `yaml:"users"`  // optional multiple credentials
	TLS           TLSConfig  `yaml:"tls"`
}

// UserSpec is an optional per-listener credential.
type UserSpec struct {
	UUID     string `yaml:"uuid"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
}

// TLSConfig configures TLS for listeners that require a certificate.
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// HealthConfig configures health monitoring.
type HealthConfig struct {
	Enabled         bool          `yaml:"enabled"`
	Workers         int           `yaml:"workers"`              // health worker pool size (default: 50)
	ProbeTimeout    time.Duration `yaml:"probe_timeout"`        // per-probe timeout (default: 3s)
	CandidateWindow time.Duration `yaml:"candidate_window"`     // frequency for candidate pool (default: 1m)
	FleetInterval   time.Duration `yaml:"fleet_interval"`       // baseline sweep for full fleet (default: 24h)
	DeepInterval    time.Duration `yaml:"deep_interval"`        // deep/through-proxy probes (default: 15m)
	MaxProbeBytes   MiB           `yaml:"max_probe_mb_per_day"` // daily probe bandwidth budget in MiB (default: 100)
	CandidatePool   int           `yaml:"candidate_pool_size"`  // size of the deep-probe candidate pool (default: 100)
	MinInterval     time.Duration `yaml:"min_interval"`         // min gap between probes of the same node (default: 30s)
}

// SpeedConfig configures throughput estimation.
type SpeedConfig struct {
	Enabled      bool          `yaml:"enabled"`
	Interval     time.Duration `yaml:"interval"`            // much rarer than health checks (default: 6h)
	DownloadSize int64         `yaml:"download_size_bytes"` // bytes to fetch per test (default: 1MiB)
	Concurrency  int           `yaml:"concurrency"`         // concurrent speed tests (default: 3)
	Target       string        `yaml:"target_url"`          // small test URL (default: https://www.gstatic.com/generate_204)
	MaxPerDay    int64         `yaml:"max_bytes_per_day"`   // speed-test bandwidth budget (default: 50MiB)
	Timeout      time.Duration `yaml:"timeout"`
}

// ScoringConfig configures the score engine.
type ScoringConfig struct {
	LatencyWeight    float64       `yaml:"weight_latency"`
	StabilityWeight  float64       `yaml:"weight_stability"`
	ThroughputWeight float64       `yaml:"weight_throughput"`
	LossWeight       float64       `yaml:"weight_loss"`
	EWMASamples      int           `yaml:"ewma_samples"`      // moving-average window (default: 20)
	StabilityWindow  int           `yaml:"stability_window"`  // recent checks considered for stability (default: 30)
	MinSamples       int           `yaml:"min_samples"`       // confidence gate before selectable (default: 3)
	SwitchDelta      float64       `yaml:"switch_delta"`      // fractional improvement required to switch (default: 0.10)
	MinTicks         int           `yaml:"min_ticks"`         // consecutive evaluations to confirm (default: 3)
	DwellTime        time.Duration `yaml:"dwell_time"`        // minimum time between switches (default: 5m)
	FailoverFailures int           `yaml:"failover_failures"` // consecutive failures before confirmed failover (default: 3)
	HardLatency      time.Duration `yaml:"hard_latency_ms"`   // latency above which node is considered failed (default: 3s)
	StaleAfter       time.Duration `yaml:"stale_after"`       // score freshness gate: node unrprobed this long is ineligible (default: 24h)
}

// SelectorConfig configures the automatic selector.
type SelectorConfig struct {
	Mode string `yaml:"mode"` // auto | manual (default: auto)
}

// StorageConfig configures persistence.
type StorageConfig struct {
	Path string `yaml:"path"` // SQLite database path (default: data/smart-gateway.db)
}

// WebhookConfig is a generic alerting webhook target.
type WebhookConfig struct {
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	Timeout time.Duration     `yaml:"timeout"`
}

// TelegramConfig configures Telegram alert delivery.
type TelegramConfig struct {
	Enabled       bool   `yaml:"enabled"`
	BotToken      string `yaml:"bot_token"`
	ChatID        string `yaml:"chat_id"`
	MessagePrefix string `yaml:"message_prefix"`
}

// AlertingConfig configures alert delivery.
type AlertingConfig struct {
	Webhooks []WebhookConfig `yaml:"webhooks"`
	Telegram TelegramConfig  `yaml:"telegram"`
}

// setDefaults fills every unset field with its documented default.
func (c *Config) setDefaults() {
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	c.Log.JSON = true
	if c.API.Listen == "" {
		c.API.Listen = "0.0.0.0:8080"
	}
	c.API.Dashboard = true
	if c.Core.SingboxPath == "" {
		c.Core.SingboxPath = "/usr/local/bin/sing-box"
	}
	if c.Core.APIListen == "" {
		c.Core.APIListen = "127.0.0.1:9090"
	}
	if c.Core.CacheFile == "" {
		c.Core.CacheFile = "data/core-cache.db"
	}
	if c.Core.ConfigDir == "" {
		c.Core.ConfigDir = "data/core"
	}
	if c.Core.RestartLimit == 0 {
		c.Core.RestartLimit = 5
	}
	if c.Subscriptions.File == "" {
		c.Subscriptions.File = "subscriptions.txt"
	}
	if c.Subscriptions.Refresh <= 0 {
		c.Subscriptions.Refresh = 6 * time.Hour
	}
	if c.Subscriptions.RequestTimeout <= 0 {
		c.Subscriptions.RequestTimeout = 30 * time.Second
	}
	if c.Subscriptions.MaxBytes == 0 {
		c.Subscriptions.MaxBytes = 10 << 20
	}
	if c.Subscriptions.UserAgent == "" {
		c.Subscriptions.UserAgent = "SmartGateway/1.0"
	}
	c.Subscriptions.VerifyTLS = true
	if c.Subscriptions.BackoffBase <= 0 {
		c.Subscriptions.BackoffBase = time.Minute
	}
	if c.Subscriptions.BackoffMax <= 0 {
		c.Subscriptions.BackoffMax = 4 * time.Hour
	}
	if c.Subscriptions.RetryLimit == 0 {
		c.Subscriptions.RetryLimit = 5
	}
	c.Health.Enabled = true
	if c.Health.Workers == 0 {
		c.Health.Workers = 50
	}
	if c.Health.ProbeTimeout <= 0 {
		c.Health.ProbeTimeout = 3 * time.Second
	}
	if c.Health.CandidateWindow <= 0 {
		c.Health.CandidateWindow = time.Minute
	}
	if c.Health.FleetInterval <= 0 {
		c.Health.FleetInterval = 24 * time.Hour
	}
	if c.Health.DeepInterval <= 0 {
		c.Health.DeepInterval = 15 * time.Minute
	}
	if c.Health.MaxProbeBytes == 0 {
		c.Health.MaxProbeBytes = 100
	}
	if c.Health.CandidatePool == 0 {
		c.Health.CandidatePool = 100
	}
	if c.Health.MinInterval <= 0 {
		c.Health.MinInterval = 30 * time.Second
	}
	c.Speed.Enabled = true
	if c.Speed.Interval <= 0 {
		c.Speed.Interval = 6 * time.Hour
	}
	if c.Speed.DownloadSize == 0 {
		c.Speed.DownloadSize = 1 << 20
	}
	if c.Speed.Concurrency == 0 {
		c.Speed.Concurrency = 3
	}
	if c.Speed.Target == "" {
		c.Speed.Target = "https://www.gstatic.com/generate_204"
	}
	if c.Speed.MaxPerDay == 0 {
		c.Speed.MaxPerDay = 50 << 20
	}
	if c.Speed.Timeout <= 0 {
		c.Speed.Timeout = 15 * time.Second
	}
	if c.Scoring.EWMASamples == 0 {
		c.Scoring.EWMASamples = 20
	}
	if c.Scoring.LatencyWeight == 0 && c.Scoring.StabilityWeight == 0 && c.Scoring.ThroughputWeight == 0 && c.Scoring.LossWeight == 0 {
		c.Scoring.LatencyWeight = 0.45
		c.Scoring.StabilityWeight = 0.30
		c.Scoring.ThroughputWeight = 0.15
		c.Scoring.LossWeight = 0.10
	}
	if c.Scoring.StabilityWindow == 0 {
		c.Scoring.StabilityWindow = 30
	}
	if c.Scoring.MinSamples == 0 {
		c.Scoring.MinSamples = 3
	}
	if c.Scoring.SwitchDelta == 0 {
		c.Scoring.SwitchDelta = 0.10
	}
	if c.Scoring.MinTicks == 0 {
		c.Scoring.MinTicks = 3
	}
	if c.Scoring.DwellTime <= 0 {
		c.Scoring.DwellTime = 5 * time.Minute
	}
	if c.Scoring.FailoverFailures == 0 {
		c.Scoring.FailoverFailures = 3
	}
	if c.Scoring.HardLatency <= 0 {
		c.Scoring.HardLatency = 3 * time.Second
	}
	if c.Scoring.StaleAfter <= 0 {
		c.Scoring.StaleAfter = 24 * time.Hour
	}
	if c.Selector.Mode == "" {
		c.Selector.Mode = "auto"
	}
	if c.Storage.Path == "" {
		c.Storage.Path = "data/smart-gateway.db"
	}
	if c.Speed.Interval <= 0 {
		c.Speed.Interval = 6 * time.Hour
	}
}

// normalize applies derived defaults that can only be computed after the
// YAML document has been parsed (per-listener fields, etc.).
func (c *Config) normalize() {
	for i := range c.Inbound.Listeners {
		l := &c.Inbound.Listeners[i]
		if l.Protocol == "vless-reality" && l.ShortIDLength == 0 {
			l.ShortIDLength = 8
		}
	}
}
