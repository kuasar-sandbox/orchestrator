package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// loadYAMLConfig keeps the original single-document decoder as the only owner
// of field decoding, defaults and validation. A single path bypasses merging,
// including its existing handling of nulls, aliases and empty input.
func loadYAMLConfig[T any](pathList, kind string, decode func(io.Reader) (*T, error)) (*T, error) {
	paths := strings.Split(pathList, ":")
	for i, path := range paths {
		if path == "" {
			return nil, fmt.Errorf("%s: empty configuration path at position %d in %q", kind, i+1, pathList)
		}
	}
	var reader io.Reader
	if len(paths) == 1 {
		file, err := os.Open(paths[0])
		if err != nil {
			return nil, fmt.Errorf("%s: read %s: %w", kind, paths[0], err)
		}
		defer file.Close()
		reader = file
	} else {
		raw, err := mergeYAMLFiles(paths, reflect.TypeFor[T]())
		if err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		reader = bytes.NewReader(raw)
	}
	config, err := decode(reader)
	if err != nil {
		return nil, fmt.Errorf("%s: parse %s: %w", kind, pathList, err)
	}
	return config, nil
}

func mergeYAMLFiles(paths []string, schema reflect.Type) ([]byte, error) {
	var merged *yaml.Node
	total := 0
	for i, path := range paths {
		raw, err := readYAMLFile(path, maxConfigBytes-total)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		total += len(raw)
		node, err := parseYAMLLayer(raw, schema)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		if i == 0 {
			merged = node // The first document's nulls are values, not deletions.
		} else {
			merged = mergeYAMLNodes(merged, node, schema)
		}
	}
	var output limitedYAMLBuffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	if err := encoder.Encode(merged); err != nil {
		return nil, fmt.Errorf("merge %s: %w", strings.Join(paths, ":"), err)
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func readYAMLFile(path string, remaining int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, int64(remaining)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > remaining {
		return nil, fmt.Errorf("combined configuration exceeds %d bytes", maxConfigBytes)
	}
	return raw, nil
}

// The encoder expands shared nodes incrementally into a bounded writer; aliases
// cannot allocate an unbounded expanded byte slice before the limit is checked.
type limitedYAMLBuffer struct{ bytes.Buffer }

func (b *limitedYAMLBuffer) Write(p []byte) (int, error) {
	if len(p) > maxConfigBytes-b.Len() {
		return 0, fmt.Errorf("merged configuration exceeds %d bytes", maxConfigBytes)
	}
	return b.Buffer.Write(p)
}

func parseYAMLLayer(raw []byte, schema reflect.Type) (*yaml.Node, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple YAML documents are not allowed")
		}
		return nil, err
	}
	// Node parsing alone does not detect duplicate keys or invalid merge keys.
	// Use yaml.v3's ordinary structural checks, including its alias expansion
	// guard, for each file before a later layer can hide an error. This value is
	// discarded: all merging retains the original scalar tags and text below.
	var structure any
	if err := document.Decode(&structure); err != nil {
		return nil, err
	}
	normalizer := yamlLayerNormalizer{done: make(map[yamlNodeSchema]*yaml.Node), active: make(map[*yaml.Node]bool)}
	return normalizer.normalize(document.Content[0], schema)
}

// Normalized nodes may share immutable subtrees. Cross-file merging is
// copy-on-write, so changing an anchor's defining field never changes another
// field that referred to its old value in the same input file.
type yamlNodeSchema struct {
	node   *yaml.Node
	schema reflect.Type
}

type yamlLayerNormalizer struct {
	done   map[yamlNodeSchema]*yaml.Node
	active map[*yaml.Node]bool
}

func (n *yamlLayerNormalizer) normalize(in *yaml.Node, schema reflect.Type) (*yaml.Node, error) {
	schema = yamlIndirectType(schema)
	identity := yamlNodeSchema{in, schema}
	if out, ok := n.done[identity]; ok {
		return out, nil
	}
	if n.active[in] {
		return nil, fmt.Errorf("line %d: cyclic YAML alias", in.Line)
	}
	n.active[in] = true
	defer delete(n.active, in)
	if in.Kind == yaml.AliasNode {
		return n.normalize(in.Alias, schema) // Native structural decoding rejected cycles.
	}
	out := *in
	out.Anchor, out.HeadComment, out.LineComment, out.FootComment = "", "", "", ""
	out.Content = nil
	if in.Kind == yaml.MappingNode {
		entries, err := yamlMappingEntries(in, schema)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			value, err := n.normalize(entry.value, yamlFieldType(schema, entry.key))
			if err != nil {
				return nil, err
			}
			out.Content = append(out.Content, entry.key, value)
		}
	} else {
		for _, child := range in.Content {
			value, err := n.normalize(child, yamlElementType(schema))
			if err != nil {
				return nil, err
			}
			out.Content = append(out.Content, value)
		}
	}
	n.done[identity] = &out
	return &out, nil
}

type yamlMappingEntry struct {
	key, value *yaml.Node
}

// Let yaml.v3 select effective values (including << precedence and duplicate
// key handling) in the destination's key type. Only keys are canonicalized;
// scalar values retain their original YAML nodes, including numeric spelling.
func yamlMappingEntries(node *yaml.Node, schema reflect.Type) ([]yamlMappingEntry, error) {
	entries := []yamlMappingEntry{}
	appendEntry := func(key any, value yaml.Node) error {
		var keyNode yaml.Node
		if err := keyNode.Encode(key); err != nil {
			return err
		}
		entries = append(entries, yamlMappingEntry{&keyNode, &value})
		return nil
	}
	if yamlStringKeys(schema) {
		// yaml.v3 rejects a repeated explicit struct field even when two alias
		// keys have different source spellings. A map decoder alone would hide it.
		if schema.Kind() == reflect.Struct {
			seen := map[string]bool{}
			for i := 0; i < len(node.Content); i += 2 {
				keyNode := node.Content[i]
				if keyNode.Kind == yaml.ScalarNode && keyNode.ShortTag() == "!!merge" {
					continue
				}
				var key string
				if err := keyNode.Decode(&key); err != nil {
					return nil, err
				}
				if seen[key] {
					return nil, fmt.Errorf("line %d: field %s already set", keyNode.Line, key)
				}
				seen[key] = true
			}
		}
		var fields map[string]yaml.Node
		if err := node.Decode(&fields); err != nil {
			return nil, err
		}
		for key, value := range fields {
			if err := appendEntry(key, value); err != nil {
				return nil, err
			}
		}
	} else {
		var fields map[any]yaml.Node
		if err := node.Decode(&fields); err != nil {
			return nil, err
		}
		for key, value := range fields {
			if err := appendEntry(key, value); err != nil {
				return nil, err
			}
		}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		left, right := entries[i].key, entries[j].key
		if left.Tag != right.Tag {
			return left.Tag < right.Tag
		}
		return left.Value < right.Value
	})
	return entries, nil
}

func yamlStringKeys(schema reflect.Type) bool {
	schema = yamlIndirectType(schema)
	return schema != nil && (schema.Kind() == reflect.Struct || schema.Kind() == reflect.Map && schema.Key().Kind() == reflect.String)
}

// A struct or map[string] field consumes numeric-looking keys as strings;
// an untyped Collector subtree retains yaml.v3's native key types. The key's
// original scalar values are never rewritten during identity comparison.
func yamlMappingKey(node *yaml.Node, schema reflect.Type) (any, error) {
	schema = yamlIndirectType(schema)
	if yamlStringKeys(schema) {
		var key string
		if err := node.Decode(&key); err != nil {
			return nil, err
		}
		return key, nil
	}
	var key any
	if err := node.Decode(&key); err != nil {
		return nil, err
	}
	if key != nil && !reflect.TypeOf(key).Comparable() {
		return nil, fmt.Errorf("line %d: invalid YAML mapping key", node.Line)
	}
	return key, nil
}

func mergeYAMLNodes(base, overlay *yaml.Node, schema reflect.Type) *yaml.Node {
	if overlay.Kind != yaml.MappingNode {
		return overlay
	}
	out := *overlay
	out.Content = nil
	positions := make(map[any]int)
	if base != nil && base.Kind == yaml.MappingNode {
		out.Content = append(out.Content, base.Content...)
		for i := 0; i < len(out.Content); i += 2 {
			key, _ := yamlMappingKey(out.Content[i], schema)
			positions[key] = i
		}
	}
	for i := 0; i < len(overlay.Content); i += 2 {
		keyNode, value := overlay.Content[i], overlay.Content[i+1]
		key, _ := yamlMappingKey(keyNode, schema)
		position, exists := positions[key]
		if value.Kind == yaml.ScalarNode && value.ShortTag() == "!!null" {
			if exists {
				out.Content[position], out.Content[position+1] = nil, nil
				delete(positions, key)
			}
			continue
		}
		if exists {
			out.Content[position+1] = mergeYAMLNodes(out.Content[position+1], value, yamlFieldType(schema, keyNode))
		} else {
			positions[key] = len(out.Content)
			out.Content = append(out.Content, keyNode, mergeYAMLNodes(nil, value, yamlFieldType(schema, keyNode)))
		}
	}
	content := out.Content[:0]
	for i := 0; i < len(out.Content); i += 2 {
		if out.Content[i] != nil {
			content = append(content, out.Content[i], out.Content[i+1])
		}
	}
	out.Content = content
	return &out
}

// These types only determine mapping-key identity. They never decode a field,
// apply a default, or validate an incomplete fragment.
func yamlIndirectType(schema reflect.Type) reflect.Type {
	for schema != nil && schema.Kind() == reflect.Pointer {
		schema = schema.Elem()
	}
	return schema
}

func yamlElementType(schema reflect.Type) reflect.Type {
	schema = yamlIndirectType(schema)
	if schema != nil && (schema.Kind() == reflect.Slice || schema.Kind() == reflect.Array) {
		return schema.Elem()
	}
	return nil
}

func yamlFieldType(schema reflect.Type, key *yaml.Node) reflect.Type {
	schema = yamlIndirectType(schema)
	if schema == nil {
		return nil
	}
	if schema.Kind() == reflect.Map {
		return schema.Elem()
	}
	if schema.Kind() != reflect.Struct {
		return nil
	}
	var name string
	if key.Decode(&name) != nil {
		return nil
	}
	for i := 0; i < schema.NumField(); i++ {
		field := schema.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if tag == "" {
			tag = strings.ToLower(field.Name)
		}
		if tag == name {
			return field.Type
		}
	}
	return nil
}
