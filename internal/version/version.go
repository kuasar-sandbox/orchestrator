// Package version implements the product binaries' top-level version query.
package version

import (
	"fmt"
	"os"
	"runtime/debug"
)

// Set by the owning component's build, not by the aggregate.
var releaseVersion = "dev"
var gitTag string

func identity(name, release, tag string, info *debug.BuildInfo) string {
	revision := ""
	modified := false
	if info != nil {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
	}
	source := "unknown"
	if revision != "" {
		if tag != "" {
			source = tag
		} else {
			source = revision
			if len(source) > 12 {
				source = source[:12]
			}
		}
	}
	if modified {
		source += "-modified"
	}
	return fmt.Sprintf("%s %s (%s)", name, release, source)
}

// PrintIfRequested handles only an explicit top-level --version query.
// It must run before normal command processing, but after any private worker
// bootstrap dispatch which takes precedence over user CLI arguments.
func PrintIfRequested(name string) bool {
	if len(os.Args) < 2 || os.Args[1] != "--version" {
		return false
	}
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s --version\n", name)
		os.Exit(2)
	}
	info, _ := debug.ReadBuildInfo()
	fmt.Println(identity(name, releaseVersion, gitTag, info))
	return true
}
