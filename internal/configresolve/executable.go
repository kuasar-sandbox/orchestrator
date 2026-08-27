package configresolve

import (
	"fmt"
	"os"
	"path/filepath"
)

// ValidateComponentExecutable validates the node-ctl dispatch target against
// the exact node-ctl file identity. It follows symlinks via os.Stat and rejects
// mutable group/world-writable targets before any exec attempt.
func ValidateComponentExecutable(component, nodeCtl string) error {
	if component == "" {
		return nil
	}
	if !filepath.IsAbs(component) {
		return fmt.Errorf("component executable must be absolute")
	}
	componentInfo, err := os.Stat(component)
	if err != nil {
		return fmt.Errorf("stat component executable: %w", err)
	}
	if !componentInfo.Mode().IsRegular() {
		return fmt.Errorf("component executable must be a regular file")
	}
	if componentInfo.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("component executable is not executable")
	}
	if componentInfo.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("component executable must not be group/world writable")
	}
	if nodeCtl == "" {
		return fmt.Errorf("node-ctl executable path is required")
	}
	nodeInfo, err := os.Stat(nodeCtl)
	if err != nil {
		return fmt.Errorf("stat node-ctl executable: %w", err)
	}
	if os.SameFile(componentInfo, nodeInfo) {
		return fmt.Errorf("component executable must not be the node-ctl executable")
	}
	return nil
}
