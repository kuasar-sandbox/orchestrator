package buildcfg

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func number(value string) *json.Number {
	n := json.Number(value)
	return &n
}

func TestParseFirstClassResourcesAliases(t *testing.T) {
	patch, err := ParseFirstClassResources(number("1.0001"), number("1.0001"), number("256"), number("256"))
	if err != nil {
		t.Fatal(err)
	}
	if patch.CPU == nil || *patch.CPU != 1001 {
		t.Fatalf("CPU = %v, want conservative 1001 milli-CPU", patch.CPU)
	}
	if patch.Memory == nil || *patch.Memory != 256<<20 {
		t.Fatalf("memory = %v, want 256MiB", patch.Memory)
	}
	for _, test := range []struct {
		name                                   string
		cpuCamel, cpuSnake, memCamel, memSnake *json.Number
		want                                   string
	}{
		{"cpu conflict", number("1"), number("2"), number("1"), nil, "cpuCount conflicts with cpu_count"},
		{"memory conflict", number("1"), nil, number("1"), number("2"), "memoryMB conflicts with memory_mb"},
		{"zero CPU", number("0"), nil, number("1"), nil, "cpuCount must be a positive"},
		{"negative CPU", number("-1"), nil, number("1"), nil, "cpuCount must be a positive"},
		{"NaN CPU", number("NaN"), nil, number("1"), nil, "cpuCount must be a positive"},
		{"Inf CPU", number("Inf"), nil, number("1"), nil, "cpuCount must be a positive"},
		{"fractional memory", number("1"), nil, number("1.5"), nil, "memoryMB must be a positive integer"},
		{"memory overflow", number("1"), nil, number("8796093022208"), nil, "memoryMB overflows bytes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseFirstClassResources(test.cpuCamel, test.cpuSnake, test.memCamel, test.memSnake)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestParseFirstClassResourceJSONPreservesPresence(t *testing.T) {
	patch, err := ParseFirstClassResourceJSON([]byte(`2`), nil, []byte(`2048`), nil)
	if err != nil || patch.CPU == nil || *patch.CPU != 2000 || patch.Memory == nil || *patch.Memory != 2<<30 {
		t.Fatalf("patch = %+v, err=%v", patch, err)
	}
	for name, raw := range map[string]json.RawMessage{
		"cpuCount": []byte(`null`),
		"memoryMB": []byte(`null`),
	} {
		var err error
		if name == "cpuCount" {
			_, err = ParseFirstClassResourceJSON(raw, nil, []byte(`1`), nil)
		} else {
			_, err = ParseFirstClassResourceJSON([]byte(`1`), nil, raw, nil)
		}
		if err == nil || !strings.Contains(err.Error(), name+" must not be null") {
			t.Fatalf("%s error = %v", name, err)
		}
	}
}

func TestParseBuilderResourceObjectStrict(t *testing.T) {
	valid, err := ParseResourceObject("kuasar-sandbox.builder.resources", []byte(`{"cpu":2.0001,"memory":"8GiB","storage":"64GiB"}`))
	if err != nil {
		t.Fatal(err)
	}
	if valid.CPU == nil || *valid.CPU != 2001 || valid.Memory == nil || *valid.Memory != 8<<30 || valid.Storage == nil || *valid.Storage != 64<<30 {
		t.Fatalf("normalized resources = %+v", valid)
	}

	for _, test := range []struct {
		name string
		raw  string
		want string
	}{
		{"top null", `null`, "must not be null"},
		{"leaf null", `{"cpu":null}`, "$.cpu"},
		{"array", `[]`, "must be a JSON object"},
		{"scalar", `1`, "must be a JSON object"},
		{"trailing", `{} {}`, "trailing"},
		{"unknown", `{"gpu":1}`, "unknown field"},
		{"duplicate", `{"cpu":1,"cpu":2}`, "duplicate key"},
		{"zero", `{"cpu":0}`, ".cpu must be a positive"},
		{"negative", `{"cpu":-1}`, ".cpu must be a positive"},
		{"empty size", `{"memory":""}`, ".memory must be a non-empty"},
		{"invalid size", `{"memory":"lots"}`, ".memory is invalid"},
		{"zero size", `{"storage":"0"}`, ".storage must be > 0"},
		{"number size", `{"memory":123}`, ".memory must be a size string"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseResourceObject("kuasar-sandbox.builder.resources", []byte(test.raw))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestMergeBuildResourcesRequiresEqualAssertions(t *testing.T) {
	body := ResourcePatch{CPU: int64Pointer(2000), Memory: int64Pointer(2 << 30)}
	header := ResourcePatch{CPU: int64Pointer(2000), Storage: int64Pointer(64 << 30)}
	merged, err := MergeResourcePatches(body, header)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := ResolveResources(merged)
	if err != nil {
		t.Fatal(err)
	}
	if resources.CPU != 2000 || resources.Memory != 2<<30 || resources.Storage != 64<<30 {
		t.Fatalf("resources = %+v", resources)
	}
	header.CPU = int64Pointer(2001)
	if _, err := MergeResourcePatches(body, header); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestCPUConversionBounds(t *testing.T) {
	if _, err := CPUCoresToMilli("cpu", json.Number("9223372036854775.808")); err == nil {
		t.Fatal("overflowing CPU accepted")
	}
	if got, err := CPUCoresToMilli("cpu", json.Number("0.0000001")); err != nil || got != 1 {
		t.Fatalf("minimum positive CPU = %d, %v", got, err)
	}
	if _, err := CPUCoresToMilli("cpu", json.Number(strings.Repeat("9", int(math.Log10(float64(math.MaxInt64)))+20))); err == nil {
		t.Fatal("large CPU accepted")
	}
}

func int64Pointer(value int64) *int64 { return &value }
