package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Load reads and validates the configuration.
//
// Precedence (lowest to highest): defaults, YAML file, environment variables.
// The returned Config is immutable after validation (architecture §21).
// On validation failure an aggregated *Errors is returned and the caller MUST terminate (FR-003).
func Load(path string) (*Config, error) {
	cfg := &Config{}
	cfg.setDefaults()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file %s: %w", path, err)
		}
		cfg.normalize()
	}

	if err := applyEnv(cfg); err != nil {
		return nil, err
	}

	if errs := cfg.Validate(); errs != nil {
		return nil, errs
	}
	return cfg, nil
}

func envStr(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	return strings.TrimSpace(v), ok && strings.TrimSpace(v) != ""
}

func envBool(key string) (bool, bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return false, false, nil
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return false, true, fmt.Errorf("env %s: invalid boolean %q", key, v)
	}
	return b, true, nil
}

func envInt(key string) (int, bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, true, fmt.Errorf("env %s: invalid integer %q", key, v)
	}
	return n, true, nil
}

func envInt64(key string) (int64, bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("env %s: invalid integer %q", key, v)
	}
	return n, true, nil
}

func envDuration(key string) (time.Duration, bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return 0, false, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return 0, true, fmt.Errorf("env %s: invalid duration %q", key, v)
	}
	return d, true, nil
}

// applyEnv applies SG_* environment overrides on top of the loaded YAML.
// Each override is documented in docs/configuration.md.
func applyEnv(c *Config) error {
	var err error

	if v, ok := envStr("SG_LOG_LEVEL"); ok {
		c.Log.Level = v
	}
	if v, ok := envStr("SG_API_LISTEN"); ok {
		c.API.Listen = v
	}
	if v, ok := envStr("SG_API_TOKEN"); ok {
		c.API.Token = v
	}
	if err = envSetBool(&c.API.Dashboard, "SG_API_DASHBOARD"); err != nil {
		return err
	}

	if v, ok := envStr("SG_CORE_SINGBOX_PATH"); ok {
		c.Core.SingboxPath = v
	}
	if v, ok := envStr("SG_CORE_API_LISTEN"); ok {
		c.Core.APIListen = v
	}
	if v, ok := envStr("SG_CORE_API_SECRET"); ok {
		c.Core.APISecret = v
	}
	if v, ok := envStr("SG_CORE_CACHE_FILE"); ok {
		c.Core.CacheFile = v
	}
	if v, ok := envStr("SG_CORE_CONFIG_DIR"); ok {
		c.Core.ConfigDir = v
	}
	if err = envSetInt(&c.Core.RestartLimit, "SG_CORE_RESTART_LIMIT"); err != nil {
		return err
	}

	if v, ok := envStr("SG_SUBSCRIPTIONS_FILE"); ok {
		c.Subscriptions.File = v
	}
	if err = envSetDuration(&c.Subscriptions.Refresh, "SG_SUBSCRIPTIONS_REFRESH_INTERVAL"); err != nil {
		return err
	}
	if err = envSetDuration(&c.Subscriptions.RequestTimeout, "SG_SUBSCRIPTIONS_REQUEST_TIMEOUT"); err != nil {
		return err
	}
	if err = envSetInt64(&c.Subscriptions.MaxBytes, "SG_SUBSCRIPTIONS_MAX_BYTES"); err != nil {
		return err
	}
	if v, ok := envStr("SG_SUBSCRIPTIONS_USER_AGENT"); ok {
		c.Subscriptions.UserAgent = v
	}
	if err = envSetBool(&c.Subscriptions.VerifyTLS, "SG_SUBSCRIPTIONS_VERIFY_TLS"); err != nil {
		return err
	}

	if err = envSetInt(&c.Health.Workers, "SG_HEALTH_WORKERS"); err != nil {
		return err
	}
	if err = envSetDuration(&c.Health.ProbeTimeout, "SG_HEALTH_PROBE_TIMEOUT"); err != nil {
		return err
	}
	if err = envSetMiB(&c.Health.MaxProbeBytes, "SG_HEALTH_MAX_PROBE_MB_PER_DAY"); err != nil {
		return err
	}
	if err = envSetInt(&c.Health.CandidatePool, "SG_HEALTH_CANDIDATE_POOL_SIZE"); err != nil {
		return err
	}

	if err = envSetBool(&c.Speed.Enabled, "SG_SPEED_ENABLED"); err != nil {
		return err
	}
	if err = envSetInt64(&c.Speed.DownloadSize, "SG_SPEED_DOWNLOAD_SIZE_BYTES"); err != nil {
		return err
	}
	if err = envSetInt(&c.Speed.Concurrency, "SG_SPEED_CONCURRENCY"); err != nil {
		return err
	}

	if err = envSetDuration(&c.Scoring.DwellTime, "SG_SCORING_DWELL_TIME"); err != nil {
		return err
	}
	if err = envSetInt(&c.Scoring.MinTicks, "SG_SCORING_MIN_TICKS"); err != nil {
		return err
	}

	if v, ok := envStr("SG_SELECTOR_MODE"); ok {
		c.Selector.Mode = v
	}
	if v, ok := envStr("SG_STORAGE_PATH"); ok {
		c.Storage.Path = v
	}

	if err = envSetBool(&c.Alerting.Telegram.Enabled, "SG_ALERTING_TELEGRAM_ENABLED"); err != nil {
		return err
	}
	if v, ok := envStr("SG_ALERTING_TELEGRAM_BOT_TOKEN"); ok {
		c.Alerting.Telegram.BotToken = v
	}
	if v, ok := envStr("SG_ALERTING_TELEGRAM_CHAT_ID"); ok {
		c.Alerting.Telegram.ChatID = v
	}
	return nil
}

func envSetBool(target *bool, key string) error {
	v, set, err := envBool(key)
	if err != nil {
		return err
	}
	if set {
		*target = v
	}
	return nil
}

func envSetInt(target *int, key string) error {
	v, set, err := envInt(key)
	if err != nil {
		return err
	}
	if set {
		*target = v
	}
	return nil
}

func envSetInt64(target *int64, key string) error {
	v, set, err := envInt64(key)
	if err != nil {
		return err
	}
	if set {
		*target = v
	}
	return nil
}

func envSetMiB(target *MiB, key string) error {
	v, set, err := envInt64(key)
	if err != nil {
		return err
	}
	if set {
		*target = MiB(v)
	}
	return nil
}

func envSetDuration(target *time.Duration, key string) error {
	v, set, err := envDuration(key)
	if err != nil {
		return err
	}
	if set {
		*target = v
	}
	return nil
}
