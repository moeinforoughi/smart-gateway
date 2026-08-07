// Package version holds build-time identity for Smart Gateway.
package version

import (
	"runtime"
	"runtime/debug"
	"strings"
)

// Values are overridden at build time via:
//
//	-ldflags "-X smart-gateway/internal/version.Version=v1.0.0 ..."
var (
	// Version is the semantic version of the build.
	Version = "dev"
	// Commit is the VCS commit hash, if known.
	Commit = ""
	// BuildTime is an RFC3339 timestamp of the build.
	BuildTime = ""
)

// Full returns the human-readable version string including Go runtime info.
func Full() string {
	parts := []string{"smart-gateway", Version}
	if Commit != "" {
		parts = append(parts, Commit)
	}
	if BuildTime != "" {
		parts = append(parts, BuildTime)
	}
	return strings.Join(parts, " ") + " (" + runtime.Version() + ")"
}

// FromModule returns version metadata derived from the build info embedded in
// the binary, falling back to the compile-time variables.
func FromModule() string {
	if info, ok := debug.ReadBuildInfo(); ok && Version == "dev" {
		if v := info.Main.Version; v != "" && v != "(devel)" {
			return v
		}
	}
	return Version
}
