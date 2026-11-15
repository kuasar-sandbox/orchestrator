package workspaceprobe

import _ "embed"

//go:embed probe_arm64.elf
var image []byte
