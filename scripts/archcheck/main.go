// Command archcheck enforces the dependency direction of Smart Gateway
// (architecture §3): dependencies always point inward.
//
//	shared        : domain, log, config, version  (import nothing internal)
//	rank 0 (inner): domain
//	rank 1        : service, scheduler
//	rank 2        : repository
//	rank 3        : infrastructure, monitoring, dashboard
//	rank 4        : api
//	rank 5        : app
//	rank 6        : cmd/gateway
//
// Run from the repository root:  go run ./scripts/archcheck
package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

var ranks = map[string]int{
	"domain":         0,
	"config":         0,
	"log":            0,
	"version":        0,
	"service":        1,
	"scheduler":      1,
	"subscription":   1,
	"parser":         1,
	"repository":     2,
	"infrastructure": 3,
	"monitoring":     3,
	"dashboard":      3,
	"api":            4,
	"app":            5,
	"cmd":            6,
}

var shared = map[string]bool{
	"domain": true, "log": true, "config": true, "version": true,
}

const prefix = "smart-gateway/internal/"

func main() {
	violations := 0
	_ = filepath.WalkDir("internal", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "walk error: %v\n", err)
			os.Exit(1)
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		rel := filepath.ToSlash(path)
		parts := strings.Split(rel, "/")
		pkg := parts[1] // top-level package under internal/
		rank, ok := ranks[pkg]
		if !ok {
			fmt.Fprintf(os.Stderr, "internal/%s: unknown layer; update scripts/archcheck\n", pkg)
			violations++
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
			violations++
			return nil
		}
		for _, imp := range f.Imports {
			name := strings.Trim(imp.Path.Value, `"`)
			if !strings.HasPrefix(name, prefix) {
				continue
			}
			target := strings.SplitN(strings.TrimPrefix(name, prefix), "/", 2)[0]
			if shared[target] {
				continue
			}
			tr, ok := ranks[target]
			if !ok {
				continue
			}
			if tr > rank {
				fmt.Fprintf(os.Stderr, "VIOLATION %s: imports %s (rank %d > %d)\n", rel, name, tr, rank)
				violations++
			}
		}
		return nil
	})
	if violations > 0 {
		fmt.Fprintf(os.Stderr, "archcheck: %d violations\n", violations)
		os.Exit(1)
	}
	fmt.Println("archcheck: ok")
}
