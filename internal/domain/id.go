package domain

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"
)

// NewUUIDv7 returns a UUIDv7 (RFC 9562): 48 bits of Unix-ms timestamp in the
// most significant bytes and 74 random bits, carrying the version and
// variant markers. Timestamp comes from the injected clock so tests stay
// deterministic; entropy comes from crypto/rand.
func NewUUIDv7(now time.Time) string {
	ts := uint64(now.UnixMilli()) & (1<<48 - 1)
	var b [16]byte
	if _, err := rand.Read(b[6:]); err != nil {
		panic("domain: crypto/rand unavailable: " + err.Error())
	}
	b[0] = byte(ts >> 40)
	b[1] = byte(ts >> 32)
	b[2] = byte(ts >> 24)
	b[3] = byte(ts >> 16)
	b[4] = byte(ts >> 8)
	b[5] = byte(ts)
	b[6] = b[6]&0x0f | 0x70 // version 7
	b[8] = b[8]&0x3f | 0x80 // variant RFC 4122
	raw := hex.EncodeToString(b[:])
	return raw[0:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:32]
}

// NodeDedupeHash computes the stable identity of a node over its observable
// fields (FR-204, FR-205): server, port, protocol and normalized params.
// Two nodes differing in nothing but cosmetic naming collide and are treated
// as the same node during ingestion.
func NodeDedupeHash(server string, port int, protocol Protocol, params map[string]string) string {
	h := sha256.New()
	h.Write([]byte(strings.ToLower(strings.TrimSpace(server))))
	h.Write([]byte(strconv.Itoa(port)))
	h.Write([]byte(string(protocol)))
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte("="))
		h.Write([]byte(params[k]))
		h.Write([]byte(";"))
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}
