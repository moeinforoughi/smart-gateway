// Package singbox adapts Smart Gateway to the sing-box data plane
// (architecture §4). It owns config generation, the Clash API client and the
// supervised subprocess lifecycle. Selection logic never lives here
// (invariant: "proxy core never contains selection logic").
package singbox

import (
	"encoding/json"
	"errors"
	"strconv"

	"smart-gateway/internal/domain"
)

// DefaultSelectorTag is the stable tag of the smart selector outbound.
const DefaultSelectorTag = "smart"

// GeneratorOptions configures the config generator.
type GeneratorOptions struct {
	SelectorTag    string           // selector outbound tag (default: "smart")
	Inbounds       []map[string]any // pre-built inbound JSON blocks
	ClashAPIListen string           // e.g. 127.0.0.1:9090
	ClashAPISecret string
	CacheFile      string // experimental.cache_file.path
	LogLevel       string // info | debug | warn | error
}

// Generator builds sing-box configuration documents from domain nodes.
type Generator struct {
	opts GeneratorOptions
}

// NewGenerator builds a generator with defaults applied.
func NewGenerator(opts GeneratorOptions) *Generator {
	if opts.SelectorTag == "" {
		opts.SelectorTag = DefaultSelectorTag
	}
	if opts.LogLevel == "" {
		opts.LogLevel = "info"
	}
	return &Generator{opts: opts}
}

// SelectorTag returns the tag the generated selector uses.
func (g *Generator) SelectorTag() string { return g.opts.SelectorTag }

// Config is the generated sing-box configuration document.
type Config struct {
	Log          map[string]any   `json:"log,omitempty"`
	Inbounds     []map[string]any `json:"inbounds,omitempty"`
	Outbounds    []map[string]any `json:"outbounds"`
	Route        map[string]any   `json:"route"`
	Experimental map[string]any   `json:"experimental,omitempty"`
}

// Generate produces the full sing-box configuration for the given nodes.
// Nodes that cannot be expressed in sing-box (e.g. SSR) are skipped and
// reported. The smart selector is always first so the control plane can apply
// selections via the Clash API without touching the config.
func (g *Generator) Generate(nodes []domain.Node) (*Config, error) {
	outbounds := make([]map[string]any, 0, len(nodes)+1)
	tags := make([]string, 0, len(nodes))
	var skipped int
	for _, n := range nodes {
		ob, ok := g.buildOutbound(n)
		if !ok {
			skipped++
			continue
		}
		outbounds = append(outbounds, ob)
		tags = append(tags, n.ID)
	}
	if len(tags) == 0 {
		return nil, errors.New("no usable outbounds to configure")
	}
	sel := map[string]any{
		"type":                        "selector",
		"tag":                         g.opts.SelectorTag,
		"outbounds":                   tags,
		"interrupt_exist_connections": false,
	}
	all := append([]map[string]any{sel}, outbounds...)

	cfg := &Config{
		Log: map[string]any{
			"level":     g.opts.LogLevel,
			"timestamp": true,
		},
		Inbounds:  g.opts.Inbounds,
		Outbounds: all,
		Route: map[string]any{
			"final": g.opts.SelectorTag,
		},
	}
	if g.opts.ClashAPIListen != "" {
		cfg.Experimental = map[string]any{
			"clash_api": map[string]any{
				"external_controller": g.opts.ClashAPIListen,
				"secret":              g.opts.ClashAPISecret,
			},
		}
	}
	if g.opts.CacheFile != "" {
		if cfg.Experimental == nil {
			cfg.Experimental = map[string]any{}
		}
		cfg.Experimental["cache_file"] = map[string]any{
			"enabled":        true,
			"path":           g.opts.CacheFile,
			"store_selected": true,
			"store_fakeip":   false,
		}
	}
	return cfg, nil
}

// JSON renders the configuration as indented JSON.
func (c *Config) JSON() ([]byte, error) {
	return json.MarshalIndent(c, "", "  ")
}

// buildOutbound converts one domain node into a sing-box outbound block.
// The second return value is false when the node cannot be expressed.
func (g *Generator) buildOutbound(n domain.Node) (map[string]any, bool) {
	var ob map[string]any
	switch n.Protocol {
	case domain.ProtocolVless:
		ob = g.vless(n)
	case domain.ProtocolVmess:
		ob = g.vmess(n)
	case domain.ProtocolTrojan:
		ob = g.trojan(n)
	case domain.ProtocolShadowsocks:
		ob = g.shadowsocks(n)
	case domain.ProtocolHysteria2:
		ob = g.hysteria2(n)
	case domain.ProtocolTuic:
		ob = g.tuic(n)
	case domain.ProtocolSOCKS:
		ob = g.socks(n)
	case domain.ProtocolHTTP:
		ob = g.http(n)
	default:
		// SSR and unknown protocols have no sing-box representation.
		return nil, false
	}
	if ob == nil {
		return nil, false
	}
	ob["tag"] = n.ID
	return ob, true
}

func (g *Generator) base(typ string, n domain.Node) map[string]any {
	return map[string]any{
		"type":        typ,
		"server":      n.Server,
		"server_port": n.Port,
	}
}

// tlsJSON builds the TLS block, optionally for Reality.
func tlsJSON(n domain.Node, reality bool) map[string]any {
	tls := map[string]any{"enabled": true}
	if n.SNI != "" {
		tls["server_name"] = n.SNI
	}
	if n.Fingerprint != "" {
		tls["utls"] = map[string]any{
			"enabled":     true,
			"fingerprint": n.Fingerprint,
		}
	}
	if reality {
		r := map[string]any{"enabled": true}
		if n.Params["pbk"] != "" {
			r["public_key"] = n.Params["pbk"]
		}
		if n.Params["sid"] != "" {
			r["short_id"] = n.Params["sid"]
		}
		tls["reality"] = r
	}
	return tls
}

// transportJSON builds the transport block for ws/grpc/http transports.
func transportJSON(n domain.Node) map[string]any {
	switch n.Transport {
	case "ws":
		t := map[string]any{"type": "ws", "path": orDefault(n.Path, "/")}
		if n.Host != "" {
			t["headers"] = map[string]any{"Host": n.Host}
		}
		return t
	case "grpc":
		return map[string]any{"type": "grpc", "service_name": n.ServiceName}
	case "http", "h2":
		t := map[string]any{"type": "http"}
		if n.Path != "" {
			t["path"] = n.Path
		}
		if n.Host != "" {
			t["host"] = []string{n.Host}
		}
		return t
	default:
		return nil
	}
}

func (g *Generator) vless(n domain.Node) map[string]any {
	ob := g.base("vless", n)
	ob["uuid"] = n.Params["uuid"]
	if n.Flow != "" {
		ob["flow"] = n.Flow
	}
	if t := transportJSON(n); t != nil {
		ob["transport"] = t
	}
	if n.TLS {
		reality := n.Params["security"] == "reality"
		ob["tls"] = tlsJSON(n, reality)
	}
	return ob
}

func (g *Generator) vmess(n domain.Node) map[string]any {
	ob := g.base("vmess", n)
	ob["uuid"] = n.Params["uuid"]
	ob["security"] = "auto"
	if aid, err := strconv.Atoi(n.Params["alterId"]); err == nil {
		ob["alter_id"] = aid
	}
	if t := transportJSON(n); t != nil {
		ob["transport"] = t
	}
	if n.TLS {
		ob["tls"] = tlsJSON(n, false)
	}
	return ob
}

func (g *Generator) trojan(n domain.Node) map[string]any {
	ob := g.base("trojan", n)
	ob["password"] = n.Params["password"]
	if t := transportJSON(n); t != nil {
		ob["transport"] = t
	}
	if n.TLS {
		ob["tls"] = tlsJSON(n, false)
	}
	return ob
}

func (g *Generator) shadowsocks(n domain.Node) map[string]any {
	ob := g.base("shadowsocks", n)
	ob["method"] = n.Params["method"]
	ob["password"] = n.Params["password"]
	if n.Params["plugin"] != "" {
		ob["plugin"] = n.Params["plugin"]
	}
	return ob
}

func (g *Generator) hysteria2(n domain.Node) map[string]any {
	ob := g.base("hysteria2", n)
	ob["password"] = n.Params["password"]
	if n.TLS {
		ob["tls"] = tlsJSON(n, false)
	}
	if typ := n.Params["obfs"]; typ != "" {
		obfs := map[string]any{"type": typ}
		if p := n.Params["obfs-password"]; p != "" {
			obfs["password"] = p
		}
		ob["obfs"] = obfs
	}
	return ob
}

func (g *Generator) tuic(n domain.Node) map[string]any {
	ob := g.base("tuic", n)
	ob["uuid"] = n.Params["uuid"]
	ob["password"] = n.Params["password"]
	cc := n.Params["congestion_control"]
	if cc == "" {
		cc = "bbr"
	}
	ob["congestion_control"] = cc
	if n.TLS {
		ob["tls"] = tlsJSON(n, false)
	}
	return ob
}

func (g *Generator) socks(n domain.Node) map[string]any {
	ob := g.base("socks", n)
	ob["version"] = "5"
	if n.Params["user"] != "" {
		ob["username"] = n.Params["user"]
		ob["password"] = n.Params["pass"]
	}
	return ob
}

func (g *Generator) http(n domain.Node) map[string]any {
	ob := g.base("http", n)
	if n.Params["user"] != "" {
		ob["username"] = n.Params["user"]
		ob["password"] = n.Params["pass"]
	}
	if n.TLS {
		ob["tls"] = tlsJSON(n, false)
	}
	return ob
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
