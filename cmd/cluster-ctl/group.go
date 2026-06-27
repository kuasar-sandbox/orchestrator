package main

import (
	"fmt"
)

// sandboxGroupCmd used to edit a local registry state file. The cluster design
// no longer has a shared file backend; group data must come through
// SandboxGroupProvider/Importer or a registry control API.
func sandboxGroupCmd(args []string) error {
	return fmt.Errorf("sandbox-group file admin was removed; configure SandboxGroupProvider/Importer or use the registry control API")
}
