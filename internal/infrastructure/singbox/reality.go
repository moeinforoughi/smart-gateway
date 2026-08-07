package singbox

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"smart-gateway/internal/log"
)

// RealityKeypair carries the key material for vless-reality inbounds and the
// matching client configs (M5). The keypair is generated once with
// `sing-box generate reality-keypair` and persisted so the server and the
// published client URIs always agree.
type RealityKeypair struct {
	PrivateKey string
	PublicKey  string
}

// LoadOrCreateRealityKeypair reads the keypair from path or generates a fresh
// one with the sing-box binary and persists it (M5). The short_id is derived
// deterministically per listener from its short_id_len (all-zero hex), so the
// server and client configs stay in sync without extra state.
func LoadOrCreateRealityKeypair(ctx context.Context, binaryPath, path string, logger *log.Logger) (RealityKeypair, error) {
	if data, err := os.ReadFile(path); err == nil {
		var kp RealityKeypair
		if json.Unmarshal(data, &kp) == nil && kp.PrivateKey != "" && kp.PublicKey != "" {
			return kp, nil
		}
	}
	kp, err := GenerateRealityKeypair(ctx, binaryPath)
	if err != nil {
		return kp, fmt.Errorf("generate reality keypair: %w", err)
	}
	if err := persistRealityKeypair(path, kp); err != nil {
		if logger != nil {
			logger.Warn("persist reality keypair", "error", err)
		}
		return kp, nil
	}
	if logger != nil {
		logger.Info("reality keypair generated", "path", path)
	}
	return kp, nil
}

// GenerateRealityKeypair runs `sing-box generate reality-keypair` and parses
// the private/public keys from its output.
func GenerateRealityKeypair(ctx context.Context, binaryPath string) (RealityKeypair, error) {
	cmd := exec.CommandContext(ctx, binaryPath, "generate", "reality-keypair")
	out, err := cmd.Output()
	if err != nil {
		return RealityKeypair{}, err
	}
	return parseRealityKeypairOutput(out)
}

func parseRealityKeypairOutput(out []byte) (RealityKeypair, error) {
	var kp RealityKeypair
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "PrivateKey:"):
			kp.PrivateKey = strings.TrimSpace(strings.TrimPrefix(line, "PrivateKey:"))
		case strings.HasPrefix(line, "PublicKey:"):
			kp.PublicKey = strings.TrimSpace(strings.TrimPrefix(line, "PublicKey:"))
		}
	}
	if kp.PrivateKey == "" || kp.PublicKey == "" {
		return RealityKeypair{}, fmt.Errorf("unexpected `sing-box generate reality-keypair` output: %q", string(out))
	}
	return kp, nil
}

func persistRealityKeypair(path string, kp RealityKeypair) error {
	if i := strings.LastIndexAny(path, `/\`); i > 0 {
		if err := os.MkdirAll(path[:i], 0o700); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(kp, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
