package sandboxcfg

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestValidE2BAccessTokenBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name  string
		token string
		want  bool
	}{
		{name: "unspecified", token: "", want: true},
		{name: "256 ASCII bytes", token: strings.Repeat("a", 256), want: true},
		{name: "257 ASCII bytes", token: strings.Repeat("a", 257), want: false},
		{name: "256 multibyte UTF-8 bytes", token: strings.Repeat("界", 85) + "a", want: true},
		{name: "257 multibyte UTF-8 bytes", token: strings.Repeat("界", 85) + "ab", want: false},
		{name: "invalid UTF-8", token: string([]byte{0xff}), want: false},
		{name: "embedded NUL", token: "prefix\x00suffix", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidE2BAccessToken(tt.token); got != tt.want {
				t.Fatalf("ValidE2BAccessToken() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestExtractCredentialsEnforcesE2BAccessTokenBoundaries(t *testing.T) {
	for _, field := range []string{"envd_access_token", "traffic_access_token"} {
		t.Run(field, func(t *testing.T) {
			for _, tt := range []struct {
				name    string
				token   string
				wantErr bool
			}{
				{name: "256 bytes", token: strings.Repeat("界", 85) + "a"},
				{name: "257 bytes", token: strings.Repeat("界", 85) + "ab", wantErr: true},
				{name: "embedded NUL", token: "prefix\x00suffix", wantErr: true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					raw, err := json.Marshal(map[string]string{field: tt.token})
					if err != nil {
						t.Fatal(err)
					}
					_, _, err = ExtractCredentials(map[string]string{NsCredentials: string(raw)})
					if (err != nil) != tt.wantErr {
						t.Fatalf("ExtractCredentials() error = %v, want error=%t", err, tt.wantErr)
					}
				})
			}
		})
	}

	invalidUTF8 := append([]byte(`{"envd_access_token":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}`)...)
	if _, _, err := ExtractCredentials(map[string]string{NsCredentials: string(invalidUTF8)}); err == nil {
		t.Fatal("ExtractCredentials accepted an invalid UTF-8 token")
	}
}

func TestExtractCredentials(t *testing.T) {
	secret := strings.Repeat("ab", 32)
	original := map[string]string{
		NsCredentials: ` { "traffic_access_token" : "traffic", "service_secret" : "` + secret + `", "envd_access_token" : "envd" } `,
		"keep":        "value",
	}
	wantOriginal := map[string]string{
		NsCredentials: original[NsCredentials],
		"keep":        "value",
	}

	credentials, cleaned, err := ExtractCredentials(original)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.ServiceSecret != secret || credentials.EnvdAccessToken != "envd" || credentials.TrafficAccessToken != "traffic" {
		t.Fatalf("credentials = %+v", credentials)
	}
	if !reflect.DeepEqual(cleaned, map[string]string{"keep": "value"}) {
		t.Fatalf("cleaned metadata = %+v", cleaned)
	}
	if !reflect.DeepEqual(original, wantOriginal) {
		t.Fatalf("input metadata was modified: %+v", original)
	}
	cleaned["keep"] = "changed"
	if original["keep"] != "value" {
		t.Fatal("cleaned metadata aliases the input map")
	}
}

func TestValidateCredentialsForProfile(t *testing.T) {
	if err := ValidateCredentialsForProfile(types.ProfileE2B, Credentials{
		EnvdAccessToken: "envd", TrafficAccessToken: "traffic",
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCredentialsForProfile(types.ProfileBare, Credentials{}); err != nil {
		t.Fatal(err)
	}
	for _, credentials := range []Credentials{{EnvdAccessToken: "envd"}, {TrafficAccessToken: "traffic"}} {
		if err := ValidateCredentialsForProfile(types.ProfileBare, credentials); err == nil {
			t.Fatalf("bare credentials accepted: %+v", credentials)
		}
	}
	if err := ValidateCredentialsForProfile(types.Profile("other"), Credentials{}); err == nil {
		t.Fatal("invalid profile was accepted")
	}
	for _, secret := range []string{"short", strings.Repeat("A", 64), strings.Repeat("z", 64)} {
		if err := ValidateCredentialsForProfile(types.ProfileE2B, Credentials{ServiceSecret: secret}); err == nil {
			t.Fatalf("invalid service secret %q was accepted", secret)
		}
	}
}

func TestExtractCredentialsAbsent(t *testing.T) {
	if credentials, cleaned, err := ExtractCredentials(nil); err != nil || credentials != (Credentials{}) || cleaned != nil {
		t.Fatalf("nil metadata: credentials=%+v cleaned=%v err=%v", credentials, cleaned, err)
	}

	original := map[string]string{"keep": "value"}
	credentials, cleaned, err := ExtractCredentials(original)
	if err != nil || credentials != (Credentials{}) || cleaned["keep"] != "value" {
		t.Fatalf("absent namespace: credentials=%+v cleaned=%v err=%v", credentials, cleaned, err)
	}
}

func TestExtractCredentialsAllowsEmptyStrings(t *testing.T) {
	credentials, cleaned, err := ExtractCredentials(map[string]string{
		NsCredentials: `{"service_secret":"","envd_access_token":"","traffic_access_token":""}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if credentials != (Credentials{}) {
		t.Fatalf("empty overrides must mean unspecified: %+v", credentials)
	}
	if len(cleaned) != 0 {
		t.Fatalf("credentials namespace was not removed: %+v", cleaned)
	}
}

func TestExtractCredentialsRejectsInvalidObject(t *testing.T) {
	bad := map[string]string{
		"empty":                  ``,
		"null object":            `null`,
		"array":                  `[]`,
		"string":                 `"credentials"`,
		"invalid json":           `{`,
		"second json value":      `{} {}`,
		"trailing content":       `{} trailing`,
		"duplicate":              `{"envd_access_token":"one","envd_access_token":"two"}`,
		"escaped duplicate":      `{"service_secret":"","service\u005fsecret":""}`,
		"null field":             `{"envd_access_token":null}`,
		"number field":           `{"envd_access_token":1}`,
		"boolean field":          `{"traffic_access_token":true}`,
		"object field":           `{"service_secret":{}}`,
		"short service secret":   `{"service_secret":"aa"}`,
		"uppercase secret":       `{"service_secret":"` + strings.Repeat("A", 64) + `"}`,
		"non-hex service secret": `{"service_secret":"` + strings.Repeat("z", 64) + `"}`,
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			original := map[string]string{NsCredentials: raw, "keep": "value"}
			if _, cleaned, err := ExtractCredentials(original); err == nil || cleaned != nil {
				t.Fatalf("accepted %q: cleaned=%v err=%v", raw, cleaned, err)
			}
			if original[NsCredentials] != raw || original["keep"] != "value" {
				t.Fatalf("invalid parse modified input: %+v", original)
			}
		})
	}
}

func TestExtractCredentialsRejectsUnknownFields(t *testing.T) {
	for _, field := range []string{
		"unknown",
		"api_secret",
		"manifest_key",
		"stable_id",
		"forward_access_token",
		"exec_access_token",
	} {
		t.Run(field, func(t *testing.T) {
			raw := `{"` + field + `":"value"}`
			if _, _, err := ExtractCredentials(map[string]string{NsCredentials: raw}); err == nil {
				t.Fatalf("accepted forbidden field %q", field)
			}
		})
	}
}

func TestCredentialsHeaderNamespaceWinsAsWholeObject(t *testing.T) {
	metadataObject := `{"service_secret":"` + strings.Repeat("1", 64) + `","envd_access_token":"metadata"}`
	headerObject := `{"traffic_access_token":"header"}`
	request, err := MergeMetadata(
		map[string]string{NsCredentials: metadataObject},
		map[string]string{NsCredentials: headerObject},
	)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := MergeCreateMetadata(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	credentials, cleaned, err := ExtractCredentials(merged)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.ServiceSecret != "" || credentials.EnvdAccessToken != "" || credentials.TrafficAccessToken != "header" {
		t.Fatalf("credentials objects were field-merged: %+v", credentials)
	}
	if _, ok := cleaned[NsCredentials]; ok {
		t.Fatalf("credentials namespace leaked into cleaned metadata: %+v", cleaned)
	}
}

func TestMergeCreateMetadataKeepsCredentialsRequestScoped(t *testing.T) {
	defaults := map[string]string{NsCredentials: `{"envd_access_token":"default"}`, NsNetwork: "network"}
	withoutCredentials, err := MergeCreateMetadata(defaults, map[string]string{NsLaunch: "launch"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := withoutCredentials[NsCredentials]; ok {
		t.Fatalf("credentials leaked from defaults: %+v", withoutCredentials)
	}
	if withoutCredentials[NsNetwork] != "network" || withoutCredentials[NsLaunch] != "launch" {
		t.Fatalf("ordinary create metadata did not merge: %+v", withoutCredentials)
	}

	requestObject := `{"envd_access_token":"request"}`
	withCredentials, err := MergeCreateMetadata(defaults, map[string]string{NsCredentials: requestObject})
	if err != nil {
		t.Fatal(err)
	}
	if withCredentials[NsCredentials] != requestObject {
		t.Fatalf("explicit request credentials did not win: %+v", withCredentials)
	}
}
