package placement

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPlacementValidationAndWholeHeaderPrecedence(t *testing.T) {
	for _, raw := range []string{`{}`, `{"node_selectors":null}`, `{"node_selectors":[]}`, `{"node_selectors":[{}, {"zone":"a"}]}`} {
		if _, err := Parse(raw); err != nil {
			t.Fatalf("valid %s: %v", raw, err)
		}
	}
	for _, raw := range []string{`null`, `[]`, `{"node_selectors":[null]}`, `{"node_selectors":[{"zone":null}]}`, `{"node_selectors":[{"zone":1}]}`, `{"other":[]}`, `{"node_selectors":[],"node_selectors":[]}`, `{"node_selectors":[{"zone":"a","zone":"b"}]}`, `{} {}`, string([]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'})} {
		if _, err := Parse(raw); err == nil {
			t.Fatalf("invalid placement accepted: %s", raw)
		}
	}
	body := map[string]string{Metadata: `{"node_selectors":[{"zone":"a"}]}`, "keep": "value"}
	head := http.Header{Header: []string{`{}`}}
	// net/http canonicalization is part of the transport boundary.
	head = http.Header{}
	head.Set(Header, `{}`)
	clean, selected, err := Extract(body, head)
	if err != nil || len(selected.NodeSelectors) != 0 || clean[Metadata] != "" || clean["keep"] != "value" || body[Metadata] == "" {
		t.Fatalf("precedence/removal: %v %v %v", clean, selected, err)
	}
	if _, _, err := Extract(map[string]string{Metadata: `{"node_selectors":[null]}`}, head); err == nil {
		t.Fatal("valid header hid invalid metadata")
	}
	head.Add(Header, `{}`)
	if _, _, err := Extract(nil, head); err == nil {
		t.Fatal("duplicate header accepted")
	}
	head.Set(Header, strings.Repeat(" ", MaxBytes)+`{}`)
	if _, _, err := Extract(nil, head); !errors.Is(err, ErrHeaderTooLarge) {
		t.Fatalf("header size: %v", err)
	}
	if _, _, err := Extract(map[string]string{Metadata: strings.Repeat(" ", MaxBytes) + `{}`}, nil); err == nil || errors.Is(err, ErrHeaderTooLarge) {
		t.Fatalf("metadata size: %v", err)
	}
}

func TestPlacementORCannotReplaceRuntimeConstraint(t *testing.T) {
	makeTemplate := func(ref string) types.TemplateID {
		tmpl, err := types.ParseTemplateID("bare-snp-" + base64.RawURLEncoding.EncodeToString([]byte(ref)))
		if err != nil {
			t.Fatal(err)
		}
		return tmpl
	}
	base := "manifest://" + strings.Repeat("a", 64)
	legacy := makeTemplate(base)
	annotated := makeTemplate(base + "@runtime:one.bundle:digest:" + strings.Repeat("b", 64))
	request, _ := Parse(`{"node_selectors":[{}, {"zone":"a"}]}`)
	if !request.Matches(nil) || !RuntimeMatches(legacy, nil) || RuntimeMatches(annotated, nil) {
		t.Fatal("empty OR branch relaxed runtime or legacy gained filtering")
	}
	labels := map[string]string{RuntimeLabelPrefix + "two.bundle": "digest:" + strings.Repeat("b", 64)}
	if RuntimeMatches(annotated, labels) {
		t.Fatal("digest-only alias accepted")
	}
	labels[RuntimeLabelPrefix+"one.bundle"] = "digest:" + strings.Repeat("c", 64)
	if RuntimeMatches(annotated, labels) {
		t.Fatal("basename-only match accepted")
	}
	labels[RuntimeLabelPrefix+"one.bundle"] = "digest:" + strings.Repeat("b", 64)
	if !RuntimeMatches(annotated, labels) {
		t.Fatal("same basename and digest rejected")
	}
	emptyValue, _ := Parse(`{"node_selectors":[{"missing":""}]}`)
	if emptyValue.Matches(nil) {
		t.Fatal("absent label matched an empty string")
	}
}
