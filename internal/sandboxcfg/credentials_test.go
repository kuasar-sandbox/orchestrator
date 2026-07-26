package sandboxcfg

import (
	"reflect"
	"strings"
	"testing"
)

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
		"auth_sandbox_id",
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
	request := MergeMetadata(
		map[string]string{NsCredentials: metadataObject},
		map[string]string{NsCredentials: headerObject},
	)
	merged := MergeCreateMetadata(nil, request)
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
	withoutCredentials := MergeCreateMetadata(defaults, map[string]string{NsLaunch: "launch"})
	if _, ok := withoutCredentials[NsCredentials]; ok {
		t.Fatalf("credentials leaked from defaults: %+v", withoutCredentials)
	}
	if withoutCredentials[NsNetwork] != "network" || withoutCredentials[NsLaunch] != "launch" {
		t.Fatalf("ordinary create metadata did not merge: %+v", withoutCredentials)
	}

	requestObject := `{"envd_access_token":"request"}`
	withCredentials := MergeCreateMetadata(defaults, map[string]string{NsCredentials: requestObject})
	if withCredentials[NsCredentials] != requestObject {
		t.Fatalf("explicit request credentials did not win: %+v", withCredentials)
	}
}
