package listenresource

import (
	"fmt"
	"net"
	"strings"
	"unicode"
)

// Name rejects ambiguous names in both listen expressions and LISTEN_FDNAMES.
func Name(name string) error {
	if name == "" || strings.ContainsAny(name, ":\x00") || strings.IndexFunc(name, unicode.IsSpace) >= 0 {
		return fmt.Errorf("listener name %q must be nonempty and contain no whitespace, colon or NUL", name)
	}
	return nil
}

// Parse returns a name only for an explicit fd=name expression. An address is
// never replaced by an ambient file, even if a file has the same port or name.
func Parse(expression string) (name string, err error) {
	if strings.HasPrefix(expression, "fd=") {
		name = strings.TrimPrefix(expression, "fd=")
		return name, Name(name)
	}
	if _, _, err := net.SplitHostPort(expression); err != nil {
		return "", fmt.Errorf("listen %q: expected host:port or fd=<name>: %w", expression, err)
	}
	return "", nil
}
