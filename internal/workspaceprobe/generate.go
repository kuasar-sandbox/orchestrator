//go:build ignore

// Generate a sectionless Linux ELF containing only the position-independent
// entry function. The host package embeds the result without depending on a
// cross C toolchain or installing any additional product on the node.
package main

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	dir, err := os.MkdirTemp("", "workspace-probe-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	for _, arch := range []string{"amd64", "arm64"} {
		full := filepath.Join(dir, arch)
		cmd := exec.Command("go", "build", "-trimpath", "-ldflags=-E main.entry", "-o", full, "./source")
		cmd.Env = append(os.Environ(), "GOOS=linux", "CGO_ENABLED=0", "GOARCH="+arch)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
		if err := extract(full, arch); err != nil {
			log.Fatal(err)
		}
	}
}

func extract(full, arch string) error {
	in, err := elf.Open(full)
	if err != nil {
		return err
	}
	defer in.Close()
	symbols, err := in.Symbols()
	if err != nil {
		return err
	}
	for _, s := range symbols {
		if s.Name != "main.entry" {
			continue
		}
		sec := in.Sections[s.Section]
		body, err := sec.Data()
		if err != nil {
			return err
		}
		offset := s.Value - sec.Addr
		code := body[offset : offset+s.Size]
		const header = 64 + 56
		const addr = 0x400000
		var b bytes.Buffer
		h := elf.Header64{Type: uint16(elf.ET_EXEC), Machine: uint16(in.Machine), Version: 1, Entry: addr + header, Phoff: 64, Ehsize: 64, Phentsize: 56, Phnum: 1}
		copy(h.Ident[:], []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
		p := elf.Prog64{Type: uint32(elf.PT_LOAD), Flags: uint32(elf.PF_R | elf.PF_X), Vaddr: addr, Paddr: addr, Filesz: uint64(header + len(code)), Memsz: uint64(header + len(code)), Align: 4096}
		if err := binary.Write(&b, binary.LittleEndian, h); err != nil {
			return err
		}
		if err := binary.Write(&b, binary.LittleEndian, p); err != nil {
			return err
		}
		b.Write(code)
		return os.WriteFile("probe_"+arch+".elf", b.Bytes(), 0644)
	}
	return fmt.Errorf("missing assembly entry in %s", full)
}
