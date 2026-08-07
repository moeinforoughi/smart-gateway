package singbox

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"smart-gateway/internal/config"
)

func realityListener() config.ListenerConfig {
	lc := config.ListenerConfig{Protocol: "vless-reality", Port: 443}
	lc.Enabled = true
	lc.UUID = "11111111-1111-1111-1111-111111111111"
	lc.ServerNames = []string{"www.example.com"}
	lc.Dest = "www.example.com:443"
	lc.ShortIDLength = 8
	return lc
}

func TestBuildClientConfigReality(t *testing.T) {
	kp := RealityKeypair{PrivateKey: "priv", PublicKey: "pub"}
	cc, err := BuildClientConfig([]config.ListenerConfig{realityListener()}, kp, "")
	if err != nil {
		t.Fatal(err)
	}
	// The address field echoes the caller-supplied public host verbatim; the
	// per-inbound host falls back to the reality SNI.
	if cc.Address != "" {
		t.Fatalf("address = %q, want empty (no public_host configured)", cc.Address)
	}
	if len(cc.Inbounds) != 1 {
		t.Fatalf("inbounds = %d", len(cc.Inbounds))
	}
	ci := cc.Inbounds[0]
	if ci.PublicKey != "pub" || ci.ShortID != "00000000" {
		t.Fatalf("reality fields: %+v", ci)
	}
	if !strings.Contains(ci.URI, "pbk=pub") || !strings.Contains(ci.URI, "sid=00000000") ||
		!strings.Contains(ci.URI, "sni=www.example.com") || !strings.HasPrefix(ci.URI, "vless://") {
		t.Fatalf("share link: %q", ci.URI)
	}
	if !strings.Contains(ci.SingBox, `"public_key": "pub"`) || !strings.Contains(ci.SingBox, `"short_id": "00000000"`) {
		t.Fatalf("sing-box outbound: %s", ci.SingBox)
	}
}

func TestBuildClientConfigPublicHostWins(t *testing.T) {
	cc, err := BuildClientConfig([]config.ListenerConfig{realityListener()}, RealityKeypair{PrivateKey: "p", PublicKey: "k"}, "vpn.example.net")
	if err != nil {
		t.Fatal(err)
	}
	if cc.Address != "vpn.example.net" {
		t.Fatalf("address = %q, want override", cc.Address)
	}
	if !strings.Contains(cc.Inbounds[0].URI, "@vpn.example.net:443?") {
		t.Fatalf("uri = %q", cc.Inbounds[0].URI)
	}
}

func TestBuildClientConfigShareLinks(t *testing.T) {
	for _, tc := range []struct {
		proto   string
		listen  config.ListenerConfig
		wantPfx string
	}{
		{"vless", func() config.ListenerConfig {
			lc := config.ListenerConfig{Protocol: "vless", Port: 443, Enabled: true, UUID: "u", ServerNames: []string{"sni.example.com"}}
			return lc
		}(), "vless://u@"},
		{"trojan", func() config.ListenerConfig {
			lc := config.ListenerConfig{Protocol: "trojan", Port: 443, Enabled: true, Password: "pw", ServerNames: []string{"sni.example.com"}}
			return lc
		}(), "trojan://pw@"},
		{"shadowsocks", func() config.ListenerConfig {
			lc := config.ListenerConfig{Protocol: "shadowsocks", Port: 8388, Enabled: true, Method: "2022-blake3-aes-128-gcm", Password: "pw"}
			return lc
		}(), "ss://"},
		{"hysteria2", func() config.ListenerConfig {
			lc := config.ListenerConfig{Protocol: "hysteria2", Port: 8443, Enabled: true, Password: "pw"}
			return lc
		}(), "hysteria2://pw@"},
	} {
		cc, err := BuildClientConfig([]config.ListenerConfig{tc.listen}, RealityKeypair{}, "")
		if err != nil {
			t.Fatalf("%s: %v", tc.proto, err)
		}
		if len(cc.Inbounds) != 1 || !strings.HasPrefix(cc.Inbounds[0].URI, tc.wantPfx) {
			t.Fatalf("%s: uri = %q, want prefix %q", tc.proto, cc.Inbounds[0].URI, tc.wantPfx)
		}
	}
}

func TestBuildClientConfigSkipsDisabledAndRejectsUnknown(t *testing.T) {
	lc := realityListener()
	lc.Enabled = false
	cc, err := BuildClientConfig([]config.ListenerConfig{lc}, RealityKeypair{PublicKey: "k"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(cc.Inbounds) != 0 {
		t.Fatalf("disabled listener must be skipped, got %d", len(cc.Inbounds))
	}

	bad := config.ListenerConfig{Protocol: "ssh", Port: 22, Enabled: true}
	if _, err := BuildClientConfig([]config.ListenerConfig{bad}, RealityKeypair{}, ""); err == nil {
		t.Fatal("expected error for unsupported protocol")
	}
}

func TestRealityKeypairPersistAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keypair.json")
	kp := RealityKeypair{PrivateKey: "priv", PublicKey: "pub"}
	if err := persistRealityKeypair(path, kp); err != nil {
		t.Fatalf("persist: %v", err)
	}
	got, err := LoadOrCreateRealityKeypair(context.Background(), "unused-binary", path, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != kp {
		t.Fatalf("round-trip = %+v, want %+v", got, kp)
	}
}

func TestLoadOrCreateRealityKeypairLoadsExistingOnlyWhenValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keypair.json")
	if err := os.WriteFile(path, []byte(`{"PrivateKey":"priv"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// Invalid (missing public key): must attempt generation and fail loudly.
	_, err := LoadOrCreateRealityKeypair(context.Background(), filepath.Join(t.TempDir(), "no-such-binary"), path, nil)
	if err == nil {
		t.Fatal("expected error when persisted keypair is incomplete and binary is missing")
	}
}

func TestParseRealityKeypairOutput(t *testing.T) {
	out := "PrivateKey: priv-key\nPublicKey: pub-key\n"
	kp, err := parseRealityKeypairOutput([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	if kp.PrivateKey != "priv-key" || kp.PublicKey != "pub-key" {
		t.Fatalf("kp = %+v", kp)
	}
	if _, err := parseRealityKeypairOutput([]byte("garbage")); err == nil {
		t.Fatal("expected error on unparsable output")
	}
}

func TestZeroShortID(t *testing.T) {
	if got := zeroShortID(4); got != "0000" {
		t.Fatalf("zeroShortID(4) = %q", got)
	}
	if got := zeroShortID(0); got != "00000000" {
		t.Fatalf("zeroShortID(0) = %q", got)
	}
}
