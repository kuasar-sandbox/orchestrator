// telemetryprobe sends one real OTLP/gRPC metrics request from the guest. It
// uses Go's standard HTTP/2 transport so the fixture needs no guest packages.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"
)

// The small fixture encodes only the protobuf fields it sends; it is not an
// OTLP implementation. A real Collector receiver decodes/validates the message.
func message(field byte, content []byte) []byte {
	b := []byte{field<<3 | 2}
	b = binary.AppendUvarint(b, uint64(len(content)))
	return append(b, content...)
}

func text(field byte, value string) []byte { return message(field, []byte(value)) }

func payload() []byte {
	var resource []byte
	for _, pair := range [][2]string{{"sandbox.id", "forged-victim"}, {"sandbox.stable_id", "forged-stable"}, {"sandbox.run_id", "forged-run"}} {
		attribute := append(text(1, pair[0]), message(2, text(1, pair[1]))...)
		resource = append(resource, message(1, attribute)...)
	}
	point := binary.LittleEndian.AppendUint64([]byte{3<<3 | 1}, uint64(time.Now().UnixNano()))
	point = append(point, binary.LittleEndian.AppendUint64([]byte{4<<3 | 1}, math.Float64bits(43))...)
	metric := append(text(1, "e2e.telemetry_grpc_ingress"), text(3, "1")...)
	metric = append(metric, message(5, message(1, point))...) // Metric.gauge.data_points.
	scope := append(message(1, text(1, "telemetry-e2e")), message(2, metric)...)
	rm := append(message(1, resource), message(2, scope)...)
	return message(1, rm)
}

func run(endpoint string) error {
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	data := payload()
	frame := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(data)))
	frame = append(frame, data...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint+"/opentelemetry.proto.collector.metrics.v1.MetricsService/Export", bytes.NewReader(frame))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")
	req.Header.Set("X-Forwarded-For", "192.0.2.99")
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1025))
	if err != nil {
		return err
	}
	if response.ProtoMajor != 2 || response.StatusCode != 200 || response.Trailer.Get("Grpc-Status") != "0" || !bytes.Equal(body, make([]byte, 5)) {
		return fmt.Errorf("OTLP response: %s %s trailers=%v body=%x", response.Proto, response.Status, response.Trailer, body)
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: telemetryprobe http://HOST:PORT")
		os.Exit(2)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("OTLP_GRPC_PEER_IDENTITY_OK")
}
