package singbox

import (
	"errors"
	"fmt"

	"smart-gateway/internal/config"
)

// BuildInbound renders one configured listener as a sing-box inbound block.
// Reality inbounds require a keypair; TLS inbounds require a certificate.
func BuildInbound(lc config.ListenerConfig, kp RealityKeypair) (map[string]any, error) {
	if lc.Port < 1 || lc.Port > 65535 {
		return nil, fmt.Errorf("inbound %q: invalid port %d", lc.Protocol, lc.Port)
	}
	listen := lc.Listen
	if listen == "" {
		listen = "0.0.0.0"
	}
	tag := "in-" + lc.Protocol

	switch lc.Protocol {
	case "vless-reality":
		if kp.PrivateKey == "" {
			return nil, errors.New("vless-reality inbound requires a reality keypair")
		}
		users := make([]map[string]any, 0, len(lc.Users)+1)
		if lc.UUID != "" {
			users = append(users, map[string]any{"uuid": lc.UUID, "flow": orDefault(lc.Flow, "xtls-rprx-vision")})
		}
		for _, u := range lc.Users {
			users = append(users, map[string]any{"uuid": u.UUID, "flow": orDefault(lc.Flow, "xtls-rprx-vision")})
		}
		if len(users) == 0 {
			return nil, errors.New("vless-reality inbound requires a uuid")
		}
		shortID := zeroShortID(lc.ShortIDLength)
		return map[string]any{
			"type":        "vless",
			"tag":         tag,
			"listen":      listen,
			"listen_port": lc.Port,
			"users":       users,
			"tls": map[string]any{
				"enabled":     true,
				"server_name": lc.ServerNames,
				"reality": map[string]any{
					"enabled":     true,
					"handshake":   map[string]any{"server": lc.Dest},
					"private_key": kp.PrivateKey,
					"short_id":    []string{shortID},
				},
			},
		}, nil

	case "vless", "vmess", "trojan":
		return buildUserInbound(lc.Protocol, tag, listen, lc)
	case "shadowsocks":
		if lc.Password == "" || lc.Method == "" {
			return nil, errors.New("shadowsocks inbound requires method and password")
		}
		return map[string]any{
			"type":        "shadowsocks",
			"tag":         tag,
			"listen":      listen,
			"listen_port": lc.Port,
			"method":      lc.Method,
			"password":    lc.Password,
		}, nil

	case "hysteria2":
		if lc.Password == "" {
			return nil, errors.New("hysteria2 inbound requires a password")
		}
		return map[string]any{
			"type":        "hysteria2",
			"tag":         tag,
			"listen":      listen,
			"listen_port": lc.Port,
			"users":       []map[string]any{{"password": lc.Password}},
			"tls":         inboundTLS(lc),
		}, nil

	case "tuic":
		if lc.Password == "" && lc.UUID == "" {
			return nil, errors.New("tuic inbound requires a password or uuid")
		}
		users := []map[string]any{{"password": lc.Password, "uuid": lc.UUID}}
		return map[string]any{
			"type":               "tuic",
			"tag":                tag,
			"listen":             listen,
			"listen_port":        lc.Port,
			"users":              users,
			"congestion_control": "bbr",
			"tls":                inboundTLS(lc),
		}, nil

	default:
		return nil, fmt.Errorf("inbound protocol %q not supported", lc.Protocol)
	}
}

func buildUserInbound(protocol, tag, listen string, lc config.ListenerConfig) (map[string]any, error) {
	users := make([]map[string]any, 0, len(lc.Users)+1)
	if lc.UUID != "" {
		users = append(users, map[string]any{"uuid": lc.UUID})
	}
	for _, u := range lc.Users {
		users = append(users, map[string]any{"uuid": u.UUID})
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("%s inbound requires a uuid", protocol)
	}
	in := map[string]any{
		"type":        protocol,
		"tag":         tag,
		"listen":      listen,
		"listen_port": lc.Port,
		"users":       users,
	}
	if lc.TLS.CertFile != "" || lc.TLS.KeyFile != "" {
		in["tls"] = inboundTLS(lc)
	}
	return in, nil
}

func inboundTLS(lc config.ListenerConfig) map[string]any {
	return map[string]any{
		"enabled":          true,
		"certificate_path": lc.TLS.CertFile,
		"key_path":         lc.TLS.KeyFile,
	}
}

// zeroShortID builds an all-zero reality short_id of the requested length
// (hex chars, even length preferred by sing-box).
func zeroShortID(length int) string {
	if length <= 0 {
		length = 8
	}
	hex := "0123456789abcdef"
	out := make([]byte, length)
	for i := range out {
		out[i] = hex[0]
	}
	return string(out)
}
