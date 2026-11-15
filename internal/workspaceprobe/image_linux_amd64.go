package workspaceprobe

import _ "embed"

//go:embed probe_amd64.elf
var image []byte
