package buildcfg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtutil "github.com/kuasar-sandbox/sandboxer/pkg/util"
)

// ResourcePatch preserves registration-time presence while normalizing every
// supplied leaf into the same integer units as types.BuildResources.
type ResourcePatch struct {
	CPU     *int64
	Memory  *int64
	Storage *int64
}

type resourceInput struct {
	CPU     json.RawMessage `json:"cpu"`
	Memory  json.RawMessage `json:"memory"`
	Storage json.RawMessage `json:"storage"`
}

// ParseResourceObject parses X-Kuasar-Sandbox-Builder.resources. The caller
// supplies the full field path so API errors identify ownership precisely.
func ParseResourceObject(path string, raw []byte) (ResourcePatch, error) {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return ResourcePatch{}, fmt.Errorf("%s must not be null", path)
	}
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return ResourcePatch{}, fmt.Errorf("%s must be a JSON object", path)
	}
	var input resourceInput
	if err := strictjson.Decode(raw, &input); err != nil {
		return ResourcePatch{}, fmt.Errorf("%s must be a strict JSON object: %w", path, err)
	}
	var out ResourcePatch
	if len(input.CPU) != 0 {
		var number json.Number
		if isJSONNull(input.CPU) {
			return ResourcePatch{}, fmt.Errorf("%s.cpu must not be null", path)
		}
		if err := strictjson.Decode(input.CPU, &number); err != nil {
			return ResourcePatch{}, fmt.Errorf("%s.cpu must be a number: %w", path, err)
		}
		value, err := CPUCoresToMilli(path+".cpu", number)
		if err != nil {
			return ResourcePatch{}, err
		}
		out.CPU = &value
	}
	if len(input.Memory) != 0 {
		value, err := parseSizeJSON(path+".memory", input.Memory)
		if err != nil {
			return ResourcePatch{}, err
		}
		out.Memory = &value
	}
	if len(input.Storage) != 0 {
		value, err := parseSizeJSON(path+".storage", input.Storage)
		if err != nil {
			return ResourcePatch{}, err
		}
		out.Storage = &value
	}
	return out, nil
}

func isJSONNull(raw []byte) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

func parseSizeJSON(path string, raw []byte) (int64, error) {
	if isJSONNull(raw) {
		return 0, fmt.Errorf("%s must not be null", path)
	}
	var value string
	if err := strictjson.Decode(raw, &value); err != nil {
		return 0, fmt.Errorf("%s must be a size string: %w", path, err)
	}
	return positiveSize(path, value)
}

// CPUCoresToMilli converts a positive decimal core count using exact rational
// arithmetic and rounds up, so normalization can never under-account CPU.
func CPUCoresToMilli(path string, number json.Number) (int64, error) {
	raw := strings.TrimSpace(number.String())
	if raw == "" {
		return 0, fmt.Errorf("%s must be a positive finite number", path)
	}
	rat, ok := new(big.Rat).SetString(raw)
	if !ok || rat.Sign() <= 0 {
		return 0, fmt.Errorf("%s must be a positive finite number", path)
	}
	rat.Mul(rat, big.NewRat(1000, 1))
	q, rem := new(big.Int).QuoRem(rat.Num(), rat.Denom(), new(big.Int))
	if rem.Sign() != 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() || q.Sign() <= 0 {
		return 0, fmt.Errorf("%s overflows milli-CPU", path)
	}
	return q.Int64(), nil
}

// MemoryMBToBytes parses the E2B first-class memory field. Fractions are not
// meaningful for an MB count and multiplication is checked before conversion.
func MemoryMBToBytes(path string, number json.Number) (int64, error) {
	raw := strings.TrimSpace(number.String())
	v, ok := new(big.Int).SetString(raw, 10)
	if !ok || v.Sign() <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", path)
	}
	v.Mul(v, big.NewInt(1<<20))
	if !v.IsInt64() || v.Sign() <= 0 {
		return 0, fmt.Errorf("%s overflows bytes", path)
	}
	return v.Int64(), nil
}

func positiveSize(path, raw string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, fmt.Errorf("%s must be a non-empty positive size", path)
	}
	value, err := rtutil.ParseSize(raw)
	if err != nil {
		return 0, fmt.Errorf("%s is invalid: %w", path, err)
	}
	if value == 0 {
		return 0, fmt.Errorf("%s must be > 0", path)
	}
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("%s overflows bytes", path)
	}
	return int64(value), nil
}

// SizeBytes exposes the same strict, checked size normalization to conductor
// configuration without creating a second parser.
func SizeBytes(path, raw string) (int64, error) { return positiveSize(path, raw) }

// MergeResourcePatches combines body and header declarations. Equal duplicate
// dimensions are assertions; unequal values are ambiguous immutable input.
func MergeResourcePatches(body, header ResourcePatch) (ResourcePatch, error) {
	out := body
	for _, leaf := range []struct {
		name string
		dst  **int64
		over *int64
	}{
		{"cpu", &out.CPU, header.CPU},
		{"memory", &out.Memory, header.Memory},
		{"storage", &out.Storage, header.Storage},
	} {
		if leaf.over == nil {
			continue
		}
		if *leaf.dst != nil && **leaf.dst != *leaf.over {
			return ResourcePatch{}, fmt.Errorf("build.resources.%s conflicts between request body and X-Kuasar-Sandbox-Builder.resources", leaf.name)
		}
		value := *leaf.over
		*leaf.dst = &value
	}
	return out, nil
}

func ResolveResources(patch ResourcePatch) (types.BuildResources, error) {
	if patch.CPU == nil {
		return types.BuildResources{}, errors.New("build.resources.cpu is required (cpuCount/cpu_count or X-Kuasar-Sandbox-Builder.resources.cpu)")
	}
	if patch.Memory == nil {
		return types.BuildResources{}, errors.New("build.resources.memory is required (memoryMB/memory_mb or X-Kuasar-Sandbox-Builder.resources.memory)")
	}
	out := types.BuildResources{CPU: *patch.CPU, Memory: *patch.Memory}
	if patch.Storage != nil {
		out.Storage = *patch.Storage
	}
	if err := out.ValidateRequired(); err != nil {
		return types.BuildResources{}, err
	}
	return out, nil
}

// ParseFirstClassResources normalizes the E2B camel/snake aliases. Providing
// both is an equality assertion after unit conversion, not last-writer-wins.
func ParseFirstClassResources(cpuCamel, cpuSnake, memoryCamel, memorySnake *json.Number) (ResourcePatch, error) {
	var out ResourcePatch
	cpu, err := normalizeAlias("cpuCount", cpuCamel, "cpu_count", cpuSnake, CPUCoresToMilli)
	if err != nil {
		return ResourcePatch{}, err
	}
	memory, err := normalizeAlias("memoryMB", memoryCamel, "memory_mb", memorySnake, MemoryMBToBytes)
	if err != nil {
		return ResourcePatch{}, err
	}
	out.CPU, out.Memory = cpu, memory
	return out, nil
}

// ParseFirstClassResourceJSON is the public-envelope boundary for the E2B
// camel/snake resource fields. RawMessage preserves absent versus explicit null
// without making unrelated optional fields in the established request envelope
// part of this strict schema.
func ParseFirstClassResourceJSON(cpuCamel, cpuSnake, memoryCamel, memorySnake json.RawMessage) (ResourcePatch, error) {
	parse := func(path string, raw json.RawMessage) (*json.Number, error) {
		if len(raw) == 0 {
			return nil, nil
		}
		if isJSONNull(raw) {
			return nil, fmt.Errorf("%s must not be null", path)
		}
		var number json.Number
		if err := strictjson.Decode(raw, &number); err != nil {
			return nil, fmt.Errorf("%s must be a number: %w", path, err)
		}
		return &number, nil
	}
	cpuCamelNumber, err := parse("cpuCount", cpuCamel)
	if err != nil {
		return ResourcePatch{}, err
	}
	cpuSnakeNumber, err := parse("cpu_count", cpuSnake)
	if err != nil {
		return ResourcePatch{}, err
	}
	memoryCamelNumber, err := parse("memoryMB", memoryCamel)
	if err != nil {
		return ResourcePatch{}, err
	}
	memorySnakeNumber, err := parse("memory_mb", memorySnake)
	if err != nil {
		return ResourcePatch{}, err
	}
	return ParseFirstClassResources(cpuCamelNumber, cpuSnakeNumber, memoryCamelNumber, memorySnakeNumber)
}

func normalizeAlias(camelName string, camel *json.Number, snakeName string, snake *json.Number, parse func(string, json.Number) (int64, error)) (*int64, error) {
	var camelValue, snakeValue *int64
	if camel != nil {
		value, err := parse(camelName, *camel)
		if err != nil {
			return nil, err
		}
		camelValue = &value
	}
	if snake != nil {
		value, err := parse(snakeName, *snake)
		if err != nil {
			return nil, err
		}
		snakeValue = &value
	}
	if camelValue != nil && snakeValue != nil && *camelValue != *snakeValue {
		return nil, fmt.Errorf("%s conflicts with %s", camelName, snakeName)
	}
	if camelValue != nil {
		return camelValue, nil
	}
	return snakeValue, nil
}

func PatchFromResources(resources types.BuildResources) ResourcePatch {
	var out ResourcePatch
	if resources.CPU > 0 {
		cpu := resources.CPU
		out.CPU = &cpu
	}
	if resources.Memory > 0 {
		memory := resources.Memory
		out.Memory = &memory
	}
	if resources.Storage > 0 {
		storage := resources.Storage
		out.Storage = &storage
	}
	return out
}
