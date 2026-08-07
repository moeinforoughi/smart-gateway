package singbox

import (
	"strings"
	"testing"

	"smart-gateway/internal/config"
)

func listener(protocol string, port int) config.ListenerConfig {
	return config.ListenerConfig{Protocol: protocol, Port: port}
}

func TestBuildInboundVlessReality(t *testing.T) {
	lc := listener("vless-reality", 443)
	lc.UUID = "u1"
	lc.ServerNames = []string{"www.microsoft.com"}
	lc.Dest = "www.microsoft.com:443"
	lc.ShortIDLength = 8
	in, err := BuildInbound(lc, RealityKeypair{PrivateKey: "priv", ShortID: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	if in["type"] != "vless" || in["listen_port"] != 443 {
		t.Fatalf("inbound: %+v", in)
	}
	tls := in["tls"].(map[string]any)
	reality := tls["reality"].(map[string]any)
	if reality["private_key"] != "priv" {
		t.Fatalf("reality private key missing: %+v", reality)
	}
	if reality["short_id"].([]string)[0] != "abc" {
		t.Fatalf("short_id wrong: %+v", reality)
	}
}

func TestBuildInboundRealityRequiresKeypair(t *testing.T) {
	lc := listener("vless-reality", 443)
	lc.UUID = "u1"
	if _, err := BuildInbound(lc, RealityKeypair{}); err == nil {
		t.Fatal("expected error without keypair")
	}
}

func TestBuildInboundRealityAutoShortID(t *testing.T) {
	lc := listener("vless-reality", 443)
	lc.UUID = "u1"
	lc.ShortIDLength = 4
	in, err := BuildInbound(lc, RealityKeypair{PrivateKey: "priv"})
	if err != nil {
		t.Fatal(err)
	}
	tls := in["tls"].(map[string]any)
	reality := tls["reality"].(map[string]any)
	sid := reality["short_id"].([]string)[0]
	if len(sid) != 4 || !strings.Contains(sid, "0") {
		t.Fatalf("auto short_id wrong: %q", sid)
	}
}

func TestBuildInboundShadowsocksValidation(t *testing.T) {
	if _, err := BuildInbound(listener("shadowsocks", 8388), RealityKeypair{}); err == nil {
		t.Fatal("expected error without method/password")
	}
	lc := listener("shadowsocks", 8388)
	lc.Method = "2022-blake3-aes-128-gcm"
	lc.Password = "pw"
	in, err := BuildInbound(lc, RealityKeypair{})
	if err != nil {
		t.Fatal(err)
	}
	if in["type"] != "shadowsocks" || in["method"] != "2022-blake3-aes-128-gcm" {
		t.Fatalf("inbound: %+v", in)
	}
}

func TestBuildInboundHysteria2AndTuic(t *testing.T) {
	hy := listener("hysteria2", 8443)
	hy.Password = "pw"
	if _, err := BuildInbound(hy, RealityKeypair{}); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildInbound(listener("hysteria2", 8443), RealityKeypair{}); err == nil {
		t.Fatal("hysteria2 requires password")
	}
	tu := listener("tuic", 8443)
	tu.UUID = "u1"
	if _, err := BuildInbound(tu, RealityKeypair{}); err != nil {
		t.Fatal(err)
	}
}

func TestBuildInboundValidation(t *testing.T) {
	if _, err := BuildInbound(listener("vless", 0), RealityKeypair{}); err == nil {
		t.Fatal("expected invalid port error")
	}
	if _, err := BuildInbound(listener("ssh", 22), RealityKeypair{}); err == nil {
		t.Fatal("expected unsupported protocol error")
	}
	if _, err := BuildInbound(listener("vless", 10086), RealityKeypair{}); err == nil {
		t.Fatal("vless requires a uuid")
	}
}

func TestBuildInboundUserListAndTLS(t *testing.T) {
	lc := listener("trojan", 443)
	lc.UUID = "u1"
	lc.Users = []config.UserSpec{{UUID: "u2"}}
	lc.TLS.CertFile = "/etc/certs/fullchain.pem"
	lc.TLS.KeyFile = "/etc/certs/key.pem"
	in, err := BuildInbound(lc, RealityKeypair{})
	if err != nil {
		t.Fatal(err)
	}
	users := in["users"].([]map[string]any)
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
	tls := in["tls"].(map[string]any)
	if tls["certificate_path"] != "/etc/certs/fullchain.pem" {
		t.Fatalf("tls: %+v", tls)
	}
}
