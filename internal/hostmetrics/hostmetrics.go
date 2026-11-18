package hostmetrics

// Small, best-effort native OTLP/HTTP metrics producer for trusted node components.
// Source identity is carried in the OTLP resource; telemetry does not derive it.
import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"os"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

const DefaultSocket = "/run/sandbox-telemetry/host.sock"

// ErrPartialAcceptance means the receiver processed the request. Retrying it
// can duplicate the points it has already accepted.
var ErrPartialAcceptance = errors.New("host telemetry partial acceptance")

type Sample struct {
	Name      string
	Unit      string
	Timestamp time.Time
	Value     float64
	// IntValue preserves a counter's full signed 64-bit precision.
	IntValue   *int64
	Attributes map[string]string
}

type Publisher struct {
	socket   string
	client   *http.Client
	resource map[string]string
	scope    string
}

func New(service, source string, resource map[string]string) *Publisher {
	socket := os.Getenv("KS_HOST_OTLP_SOCKET")
	if socket == "" {
		socket = DefaultSocket
	}
	attrs := make(map[string]string, len(resource)+2)
	for k, v := range resource {
		if v != "" {
			attrs[k] = v
		}
	}
	// Caller cannot override the identity of its actual producer.
	// Node services must remain distinguishable when a central exporter receives
	// data from multiple hosts. Run-scoped publishers provide SandboxID themselves.
	if attrs["sandbox.id"] == "" {
		if name, err := os.Hostname(); err == nil && name != "" {
			attrs["host.name"] = name
		}
	}
	attrs["service.name"] = service
	attrs["sandbox.telemetry.source"] = source
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 350 * time.Millisecond}).DialContext(ctx, "unix", socket)
		},
	}
	return &Publisher{
		socket:   socket,
		client:   &http.Client{Transport: transport, Timeout: 750 * time.Millisecond},
		resource: attrs, scope: service,
	}
}

// Group represents an independently identified observation target in one OTLP request.
// A shared producer can report node, pool and many sandbox objects without one socket per sandbox.
type Group struct {
	Resource map[string]string
	Samples  []Sample
}

// Available is an inexpensive hint for producers with an expensive collection
// step. The next tick rediscovers a restarted receiver; Emit still handles races.
func (p *Publisher) Available() bool {
	if p == nil {
		return false
	}
	info, err := os.Stat(p.socket)
	return err == nil && info.Mode()&os.ModeSocket != 0
}

func (p *Publisher) Emit(ctx context.Context, values []Sample) error {
	return p.EmitBatches(ctx, []Group{{Samples: values}})
}

func (p *Publisher) EmitBatches(ctx context.Context, groups []Group) error {
	if p == nil || len(groups) == 0 {
		return nil
	}
	if len(groups) > 64 {
		return errors.New("too many OTLP resource groups")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	metrics := pmetric.NewMetrics()
	pointCount := 0
	for _, group := range groups {
		if len(group.Samples) == 0 {
			continue
		}
		pointCount += len(group.Samples)
		if pointCount > 2048 {
			return errors.New("too many OTLP samples")
		}
		resource := metrics.ResourceMetrics().AppendEmpty()
		attrs := make(map[string]string, len(p.resource)+len(group.Resource))
		for k, v := range p.resource {
			attrs[k] = v
		}
		for k, v := range group.Resource {
			if v != "" && k != "service.name" && k != "sandbox.telemetry.source" {
				attrs[k] = v
			}
		}
		keys := make([]string, 0, len(attrs))
		for k := range attrs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			resource.Resource().Attributes().PutStr(k, attrs[k])
		}
		scope := resource.ScopeMetrics().AppendEmpty()
		scope.Scope().SetName(p.scope)
		index := map[string]int{}
		for _, value := range group.Samples {
			if value.Name == "" || value.Timestamp.IsZero() || (value.IntValue == nil && (math.IsNaN(value.Value) || math.IsInf(value.Value, 0))) {
				return errors.New("invalid host metric")
			}
			key := value.Name + "\x00" + value.Unit
			pos, ok := index[key]
			if !ok {
				pos = scope.Metrics().Len()
				index[key] = pos
				metric := scope.Metrics().AppendEmpty()
				metric.SetName(value.Name)
				metric.SetUnit(value.Unit)
				metric.SetEmptyGauge()
			}
			point := scope.Metrics().At(pos).Gauge().DataPoints().AppendEmpty()
			point.SetTimestamp(pcommon.NewTimestampFromTime(value.Timestamp))
			if value.IntValue != nil {
				point.SetIntValue(*value.IntValue)
			} else {
				point.SetDoubleValue(value.Value)
			}
			for k, v := range value.Attributes {
				point.Attributes().PutStr(k, v)
			}
		}
	}
	if metrics.DataPointCount() == 0 {
		return nil
	}
	body, err := pmetricotlp.NewExportRequestFromMetrics(metrics).MarshalProto()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/v1/metrics", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	reply, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer reply.Body.Close()
	if reply.StatusCode != http.StatusOK {
		return fmt.Errorf("host telemetry returned HTTP %d", reply.StatusCode)
	}
	responseBytes, err := io.ReadAll(io.LimitReader(reply.Body, 64<<10+1))
	if err != nil {
		return fmt.Errorf("host telemetry read response: %w", err)
	}
	if len(responseBytes) > 64<<10 {
		return errors.New("host telemetry response too large")
	}
	if len(responseBytes) == 0 {
		return nil
	}
	response := pmetricotlp.NewExportResponse()
	mediaType, _, _ := mime.ParseMediaType(reply.Header.Get("Content-Type"))
	if mediaType == "application/json" {
		err = response.UnmarshalJSON(responseBytes)
	} else {
		err = response.UnmarshalProto(responseBytes)
	}
	if err != nil {
		return fmt.Errorf("host telemetry invalid response: %w", err)
	}
	partial := response.PartialSuccess()
	if partial.RejectedDataPoints() > 0 {
		return fmt.Errorf("%w: rejected=%d: %s", ErrPartialAcceptance, partial.RejectedDataPoints(), partial.ErrorMessage())
	}
	return nil
}
