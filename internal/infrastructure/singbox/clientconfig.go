package singbox

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"smart-gateway/internal/config"
)

// ClientInbound is one generated client config for a configured listener.
type ClientInbound struct {
	Protocol  string `json:"protocol"`
	Tag       string `json:"tag"`
	Listen    string `json:"listen"`
	Port      int    `json:"port"`
	URI       string `json:"uri,omitempty"`          // v2rayN-style share link
	SingBox   string `json:"singbox_json,omitempty"` // inline sing-box outbound block
	PublicKey string `json:"public_key,omitempty"`   // reality only
	ShortID   string `json:"short_id,omitempty"`     // reality only
}

// ClientConfig is the full payload of GET /api/v1/client-config.
type ClientConfig struct {
	Address  string          `json:"address"`
	Inbounds []ClientInbound `json:"inbounds"`
}

// BuildClientConfig renders one share link + sing-box outbound per enabled
// listener. Address is the public host clients use (api.public_host), falling
// back to the reality SNI or the bind host.
func BuildClientConfig(listeners []config.ListenerConfig, kp RealityKeypair, address string) (*ClientConfig, error) {
	out := &ClientConfig{Address: address, Inbounds: make([]ClientInbound, 0, len(listeners))}
	for _, lc := range listeners {
		if !lc.Enabled {
			continue
		}
		ci, err := buildClientInbound(lc, kp, address)
		if err != nil {
			return nil, err
		}
		out.Inbounds = append(out.Inbounds, ci)
	}
	return out, nil
}

func buildClientInbound(lc config.ListenerConfig, kp RealityKeypair, address string) (ClientInbound, error) {
	host := resolveAddress(lc, kp, address)
	port := lc.Port
	name := displayName(lc, host)

	ci := ClientInbound{
		Protocol: lc.Protocol,
		Tag:      "in-" + lc.Protocol,
		Listen:   lc.Listen,
		Port:     port,
	}

	ob, err := clientOutbound(lc, kp, host)
	if err != nil {
		return ci, err
	}
	blob, _ := json.MarshalIndent(ob, "", "  ")
	ci.SingBox = string(blob)

	switch lc.Protocol {
	case "vless-reality":
		ci.PublicKey = kp.PublicKey
		ci.ShortID = zeroShortID(lc.ShortIDLength)
		ci.URI = vlessRealityURI(lc, kp, host, port, name)
	case "vless", "vmess", "trojan", "shadowsocks", "hysteria2", "tuic":
		ci.URI = shareURI(lc, host, port, name)
	default:
		return ci, fmt.Errorf("inbound protocol %q not supported", lc.Protocol)
	}
	return ci, nil
}

// resolveAddress picks the public host the client should dial.
func resolveAddress(lc config.ListenerConfig, kp RealityKeypair, address string) string {
	if address != "" {
		return address
	}
	if lc.Protocol == "vless-reality" && len(lc.ServerNames) > 0 {
		return lc.ServerNames[0]
	}
	if lc.Listen != "" && lc.Listen != "0.0.0.0" && lc.Listen != "::" {
		return lc.Listen
	}
	return "127.0.0.1"
}

func displayName(lc config.ListenerConfig, host string) string {
	if len(lc.ServerNames) > 0 {
		return fmt.Sprintf("%s@%s", lc.Protocol, lc.ServerNames[0])
	}
	return fmt.Sprintf("%s@%s", lc.Protocol, host)
}

// clientOutbound mirrors the server-side inbound as a client outbound block.
func clientOutbound(lc config.ListenerConfig, kp RealityKeypair, host string) (map[string]any, error) {
	uuid := firstUUID(lc)
	password := firstPassword(lc)
	sni := ""
	if len(lc.ServerNames) > 0 {
		sni = lc.ServerNames[0]
	}
	base := map[string]any{"server": host, "server_port": lc.Port}
	switch lc.Protocol {
	case "vless-reality":
		if uuid == "" {
			return nil, fmt.Errorf("vless-reality inbound requires a uuid")
		}
		return map[string]any{
			"type": "vless", "server": host, "server_port": lc.Port, "uuid": uuid,
			"flow": orDefault(lc.Flow, "xtls-rprx-vision"),
			"tls": map[string]any{
				"enabled": true, "server_name": sni, "utls": map[string]any{"enabled": true, "fingerprint": "chrome"},
				"reality": map[string]any{"enabled": true, "public_key": kp.PublicKey, "short_id": zeroShortID(lc.ShortIDLength)},
			},
		}, nil
	case "vless":
		if uuid == "" {
			return nil, fmt.Errorf("vless inbound requires a uuid")
		}
		ob := map[string]any{"type": "vless", "uuid": uuid, "flow": orDefault(lc.Flow, "xtls-rprx-vision")}
		mergeBase(ob, base)
		ob["tls"] = clientTLS(sni, lc)
		return ob, nil
	case "vmess":
		if uuid == "" {
			return nil, fmt.Errorf("vmess inbound requires a uuid")
		}
		ob := map[string]any{"type": "vmess", "uuid": uuid, "security": "auto"}
		mergeBase(ob, base)
		ob["tls"] = clientTLS(sni, lc)
		return ob, nil
	case "trojan":
		if password == "" {
			return nil, fmt.Errorf("trojan inbound requires a password")
		}
		ob := map[string]any{"type": "trojan", "password": password}
		mergeBase(ob, base)
		ob["tls"] = clientTLS(sni, lc)
		return ob, nil
	case "shadowsocks":
		if password == "" || lc.Method == "" {
			return nil, fmt.Errorf("shadowsocks inbound requires method and password")
		}
		ob := map[string]any{"type": "shadowsocks", "method": lc.Method, "password": password}
		mergeBase(ob, base)
		return ob, nil
	case "hysteria2":
		if password == "" {
			return nil, fmt.Errorf("hysteria2 inbound requires a password")
		}
		ob := map[string]any{"type": "hysteria2", "password": password, "tls": clientTLS(sni, lc)}
		mergeBase(ob, base)
		return ob, nil
	case "tuic":
		if password == "" && uuid == "" {
			return nil, fmt.Errorf("tuic inbound requires a password or uuid")
		}
		ob := map[string]any{"type": "tuic", "uuid": uuid, "password": password, "congestion_control": "bbr", "tls": clientTLS(sni, lc)}
		mergeBase(ob, base)
		return ob, nil
	default:
		return nil, fmt.Errorf("inbound protocol %q not supported", lc.Protocol)
	}
}

func mergeBase(ob, base map[string]any) {
	for k, v := range base {
		if _, ok := ob[k]; !ok {
			ob[k] = v
		}
	}
}

func clientTLS(sni string, lc config.ListenerConfig) map[string]any {
	if lc.TLS.CertFile == "" && lc.TLS.KeyFile == "" {
		return map[string]any{"enabled": false}
	}
	tls := map[string]any{"enabled": true}
	if sni != "" {
		tls["server_name"] = sni
	}
	return tls
}

// shareURI builds the v2rayN-style share link for one listener.
func shareURI(lc config.ListenerConfig, host string, port int, name string) string {
	uuid := firstUUID(lc)
	password := firstPassword(lc)
	hn := host
	if ip := strings.Split(host, ":"); len(ip) == 2 && strings.Contains(ip[1], ".") {
		// host:port form; strip explicit port from the host component
		hn = ip[0]
	}
	addr := joinHostPort(hn, port)
	switch lc.Protocol {
	case "vless":
		q := url.Values{}
		q.Set("encryption", "none")
		q.Set("type", "tcp")
		if sni := firstSNI(lc); sni != "" {
			q.Set("security", "tls")
			q.Set("sni", sni)
		}
		return "vless://" + uuid + "@" + addr + "?" + q.Encode() + "#" + fragment(name)
	case "vmess":
		v := map[string]any{
			"v": "2", "ps": name, "add": hn, "port": port, "id": uuid,
			"aid": "0", "scy": "auto", "net": "tcp", "type": "none",
		}
		if sni := firstSNI(lc); sni != "" {
			v["tls"] = "tls"
			v["sni"] = sni
		}
		blob, _ := json.Marshal(v)
		return "vmess://" + base64.RawURLEncoding.EncodeToString(blob)
	case "trojan":
		q := url.Values{}
		if sni := firstSNI(lc); sni != "" {
			q.Set("security", "tls")
			q.Set("sni", sni)
		}
		return "trojan://" + password + "@" + addr + "?" + q.Encode() + "#" + fragment(name)
	case "shadowsocks":
		auth := base64.RawURLEncoding.EncodeToString([]byte(lc.Method + ":" + password))
		return "ss://" + auth + "@" + addr + "#" + fragment(name)
	case "hysteria2":
		q := url.Values{}
		if sni := firstSNI(lc); sni != "" {
			q.Set("sni", sni)
		}
		q.Set("insecure", "1")
		return "hysteria2://" + password + "@" + addr + "?" + q.Encode() + "#" + fragment(name)
	case "tuic":
		q := url.Values{}
		if sni := firstSNI(lc); sni != "" {
			q.Set("sni", sni)
		}
		q.Set("congestion_control", "bbr")
		q.Set("alpn", "h3")
		return "tuic://" + uuid + ":" + password + "@" + addr + "?" + q.Encode() + "#" + fragment(name)
	}
	return ""
}

func vlessRealityURI(lc config.ListenerConfig, kp RealityKeypair, host string, port int, name string) string {
	q := url.Values{}
	q.Set("encryption", "none")
	q.Set("security", "reality")
	if len(lc.ServerNames) > 0 {
		q.Set("sni", lc.ServerNames[0])
	}
	q.Set("fp", "chrome")
	q.Set("pbk", kp.PublicKey)
	q.Set("sid", zeroShortID(lc.ShortIDLength))
	q.Set("type", "tcp")
	q.Set("flow", orDefault(lc.Flow, "xtls-rprx-vision"))
	return "vless://" + firstUUID(lc) + "@" + joinHostPort(host, port) + "?" + q.Encode() + "#" + fragment(name)
}

func joinHostPort(host string, port int) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return fmt.Sprintf("[%s]:%d", host, port)
	}
	return fmt.Sprintf("%s:%d", host, port)
}

func firstUUID(lc config.ListenerConfig) string {
	if lc.UUID != "" {
		return lc.UUID
	}
	for _, u := range lc.Users {
		if u.UUID != "" {
			return u.UUID
		}
	}
	return ""
}

func firstPassword(lc config.ListenerConfig) string {
	if lc.Password != "" {
		return lc.Password
	}
	for _, u := range lc.Users {
		if u.Password != "" {
			return u.Password
		}
	}
	return ""
}

func firstSNI(lc config.ListenerConfig) string {
	if len(lc.ServerNames) > 0 {
		return lc.ServerNames[0]
	}
	return ""
}

func fragment(name string) string {
	return url.PathEscape(name)
}
