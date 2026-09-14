package main

import (
	"encoding/binary"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestSuccessfulExport(t *testing.T) {
	encode := func(response pmetricotlp.ExportResponse) []byte {
		t.Helper()
		body, err := response.MarshalProto()
		if err != nil {
			t.Fatal(err)
		}
		return append(binary.BigEndian.AppendUint32([]byte{0}, uint32(len(body))), body...)
	}
	accepted := encode(pmetricotlp.NewExportResponse())
	rejected := pmetricotlp.NewExportResponse()
	rejected.PartialSuccess().SetRejectedDataPoints(1)
	warning := pmetricotlp.NewExportResponse()
	warning.PartialSuccess().SetErrorMessage("dropped sample")
	for _, tc := range []struct {
		name  string
		frame []byte
		want  bool
	}{
		{"absent partial success", make([]byte, 5), true},
		{"actual Collector response", accepted, true},
		{"rejected point", encode(rejected), false},
		{"error message", encode(warning), false},
		{"truncated response", accepted[:len(accepted)-1], false},
		{"extra response", append(append([]byte{}, accepted...), accepted...), false},
		{"compressed frame", []byte{1, 0, 0, 0, 0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := successfulExport(tc.frame); got != tc.want {
				t.Fatalf("successfulExport(%x) = %v, want %v", tc.frame, got, tc.want)
			}
		})
	}
}
