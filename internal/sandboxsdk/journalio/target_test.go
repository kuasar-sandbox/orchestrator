// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
package journalio

import (
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	for _, tt := range []struct {
		raw    string
		tag    string
		fields map[string]string
	}{
		{"journald=app", "app", nil},
		{"journald=a_B-9,WORKLOAD_ID=w-1,STREAM=stdout", "a_B-9", map[string]string{"WORKLOAD_ID": "w-1", "STREAM": "stdout"}},
		{"journald=app,EMPTY=,EXPR=x=y,PLUS=a+b,NOTE=a%2Cb,PCT=%252C,SPACE=%20", "app", map[string]string{"EMPTY": "", "EXPR": "x=y", "PLUS": "a+b", "NOTE": "a,b", "PCT": "%2C", "SPACE": " "}},
		{"journald=app,BINARY=%00%0A%FF,LITERAL=$HOME,UTF8=%E4%B8%AD", "app", map[string]string{"BINARY": "\x00\n\xff", "LITERAL": "$HOME", "UTF8": "中"}},
	} {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := Parse(tt.raw)
			if err != nil || got.Tag != tt.tag || !reflect.DeepEqual(got.Fields, tt.fields) {
				t.Fatalf("Parse: got=%#v err=%v want=%#v", got, err, tt.fields)
			}
		})
	}
}

func TestParseRejectsInvalidTargets(t *testing.T) {
	for _, raw := range []string{
		"", "app", "journald=", "journald=bad tag", "journald=a/b", "journald=app,", "journald=app,,A=a",
		"journald=app,A", "journald=app,=v", "journald=app,A=1,A=2", "journald=app,a=x", "journald=app,1A=x",
		"journald=app,_PID=x", "journald=app,A-B=x", "journald=app,A B=x", "journald=app,A\nB=x", "journald=app,中=x",
		"journald=app,MESSAGE=x", "journald=app,PRIORITY=0", "journald=app,SYSLOG_IDENTIFIER=x",
		"journald=app,A=%", "journald=app,A=%2", "journald=app,A=%gg", "journald=app," + strings.Repeat("A", 65) + "=x",
		"journald=app,A=" + strings.Repeat("x", MaxTargetBytes),
	} {
		if _, err := Parse(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	many := "journald=app"
	for i := 0; i <= MaxFields; i++ {
		many += ",F" + strings.Repeat("A", i) + "=x"
	}
	if _, err := Parse(many); err == nil {
		t.Fatal("accepted too many fields")
	}
}

func TestPathEscapeRoundTrip(t *testing.T) {
	for _, value := range []string{"a,b=c%2C+d", "\n\x00\xff", "hello world", "中文", "'\\\"", ""} {
		target, err := Parse("journald=app,VALUE=" + url.PathEscape(value))
		if err != nil || target.Fields["VALUE"] != value {
			t.Fatalf("roundtrip %q: %#v %v", value, target, err)
		}
	}
}

func TestTargetDoesNotReadEnvironment(t *testing.T) {
	t.Setenv("WORKLOAD_ID", "ambient")
	target, err := Parse("journald=app")
	if err != nil || len(target.Fields) != 0 {
		t.Fatalf("unexpected inherited fields: %#v %v", target, err)
	}
}

func FuzzParse(f *testing.F) {
	for _, value := range []string{"journald=app", "journald=app,A=a%2Cb", "journald=x,A=%", "journald=x,A="} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got, err := Parse(raw)
		if err == nil {
			if err := got.validate(); err != nil {
				t.Fatalf("accepted invalid target: %v", err)
			}
		}
	})
}
