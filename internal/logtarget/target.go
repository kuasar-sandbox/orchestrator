// Package logtarget formats the explicit journal output targets passed to
// sandbox-ctl. Business field selection belongs to each orchestration caller.
package logtarget

import (
	"net/url"
	"sort"
	"strings"
)

// Format serializes a complete, independent output target. The tag and field
// names are caller-owned constants, not user-supplied syntax. Values are opaque
// data: escaping prevents separators from injecting another journal field.
func Format(tag string, fields map[string]string) string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	out.WriteString("journald=")
	out.WriteString(tag)
	for _, key := range keys {
		out.WriteByte(',')
		out.WriteString(key)
		out.WriteByte('=')
		out.WriteString(url.PathEscape(fields[key]))
	}
	return out.String()
}
