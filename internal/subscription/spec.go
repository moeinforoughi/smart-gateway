// Package subscription orchestrates provider subscription refreshes
// (FR-100 series): file parsing, conditional downloads, exponential backoff,
// node ingestion and per-source schedules. It depends only on domain (and
// shared packages), so persistence ports are declared here and implemented
// by internal/repository.
package subscription

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"
)

// Spec is one subscription source (FR-101, FR-103).
type Spec struct {
	Name        string
	URL         string
	Priority    int
	Tags        []string
	Headers     map[string]string
	UpdateEvery time.Duration // per-source override of the base interval (FR-112)
	Timeout     time.Duration // per-source download timeout
}

// ParseSpecsFile reads the subscriptions file. Format (FR-101):
//
//	ProviderA = https://provider.example.com/sub/a
//	ProviderB: https://provider.example.com/sub/b
//	https://bare.example.com/sub
//
// Blank lines and lines starting with '#' are ignored.
func ParseSpecsFile(path string) ([]Spec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open subscriptions file: %w", err)
	}
	defer f.Close()

	var specs []Spec
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	line := 0
	for sc.Scan() {
		line++
		raw := strings.TrimSpace(sc.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		spec, err := parseSpecLine(raw)
		if err != nil {
			return nil, fmt.Errorf("subscriptions file line %d: %w", line, err)
		}
		specs = append(specs, spec)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read subscriptions file: %w", err)
	}
	return specs, nil
}

func parseSpecLine(raw string) (Spec, error) {
	name := ""
	url := raw
	for _, sep := range []string{" = ", "= ", ": "} {
		if i := strings.Index(raw, sep); i > 0 {
			name = strings.TrimSpace(raw[:i])
			url = strings.TrimSpace(raw[i+len(sep):])
			break
		}
	}
	if name == "" {
		// Bare URL; derive a display name from the host.
		if h := hostFromURL(url); h != "" {
			name = h
		}
	}
	if !strings.Contains(url, "://") {
		return Spec{}, fmt.Errorf("invalid URL %q", url)
	}
	return Spec{Name: name, URL: url}, nil
}

func hostFromURL(u string) string {
	i := strings.Index(u, "://")
	if i < 0 {
		return ""
	}
	rest := u[i+3:]
	if j := strings.IndexAny(rest, "/?#"); j >= 0 {
		rest = rest[:j]
	}
	if k := strings.LastIndex(rest, "@"); k >= 0 {
		rest = rest[k+1:]
	}
	if colon := strings.LastIndex(rest, ":"); colon >= 0 {
		rest = rest[:colon]
	}
	return rest
}
