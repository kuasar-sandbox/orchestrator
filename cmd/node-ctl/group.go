package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

// Group files carry business context only. Root installation remains an explicit
// admin operation, and missing files never imply a delete.
type groupFile struct {
	Group           string             `json:"group"`
	Context         *nodegroup.Context `json:"context"`
	ExpectedVersion *int64             `json:"expected_version,omitempty"`
}

func groupCmd(args []string, output io.Writer) error {
	if len(args) == 0 || args[0] != "apply" {
		return fmt.Errorf("usage: node-ctl group apply [--socket PATH] -f FILE_OR_DIRECTORY")
	}
	fs := flag.NewFlagSet("group apply", flag.ContinueOnError)
	socket := fs.String("socket", "", "control socket (or NODE_CTL_SOCKET)")
	path := fs.String("f", "", "Group JSON file or directory of .json files")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *path == "" || fs.NArg() != 0 {
		return fmt.Errorf("group apply requires -f FILE_OR_DIRECTORY and no positional arguments")
	}
	apiKey := os.Getenv("E2B_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("E2B_API_KEY is required")
	}
	files, err := groupFiles(*path)
	if err != nil {
		return err
	}
	var failures []error
	for _, file := range files {
		result, err := applyGroupFile(resolveSocket(*socket), apiKey, file)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", file, err))
			continue
		}
		if err := json.NewEncoder(output).Encode(result); err != nil {
			return errors.Join(append(failures, err)...)
		}
	}
	return errors.Join(failures...)
}

func groupFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode().IsRegular() {
		return []string{path}, nil
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("Group input must be a regular file or directory")
	}
	entries, err := os.ReadDir(path) // lexical order, no recursive scan or watch
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, filepath.Join(path, entry.Name()))
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("Group directory contains no regular .json files")
	}
	return files, nil
}

func applyGroupFile(socket, apiKey, path string) (nodegroup.Result, error) {
	var result nodegroup.Result
	f, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return result, err
	}
	if len(raw) > 1<<20 {
		return result, nodegroup.ErrTooLarge
	}
	var input groupFile
	// Context nulls follow the same normalization as PUT; the envelope itself
	// requires an object context and a nonnegative revision when supplied.
	if err := strictjson.DecodeAllowNull(raw, &input); err != nil || input.Context == nil {
		return result, fmt.Errorf("invalid Group file: expected group, context object and optional expected_version")
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if _, present := fields["expected_version"]; present && input.ExpectedVersion == nil {
		return result, fmt.Errorf("expected_version must be a nonnegative integer")
	}
	if input.ExpectedVersion != nil && *input.ExpectedVersion < 0 {
		return result, fmt.Errorf("expected_version must be a nonnegative integer")
	}
	if err := nodegroup.ValidateName(input.Group); err != nil {
		return result, err
	}
	if _, _, err := nodegroup.Normalize(*input.Context); err != nil {
		return result, fmt.Errorf("invalid Group context")
	}
	headers := map[string]string{"X-API-KEY": apiKey}
	endpoint := "/groups?" + url.Values{"group": {input.Group}}.Encode()
	if input.ExpectedVersion == nil {
		code, body, err := udsDo(socket, http.MethodGet, endpoint, headers, nil)
		if err != nil {
			return result, err
		}
		if code != http.StatusOK && code != http.StatusNotFound {
			return result, fmt.Errorf("read Group (%d): %s", code, apiMessage(body))
		}
		// A tombstone's authorized 404 contains its current version. Never
		// convert an absent/malformed revision into an unconditional create.
		var revision struct {
			Version *int64 `json:"version"`
		}
		if err := strictjson.DecodeAllowUnknown(body, &revision); err != nil || revision.Version == nil || *revision.Version < 0 {
			return result, fmt.Errorf("Group read did not return a valid version")
		}
		input.ExpectedVersion = revision.Version
	}
	code, body, err := udsDo(socket, http.MethodPut, endpoint, headers, nodegroup.PutRequest{
		ExpectedVersion: *input.ExpectedVersion, Context: *input.Context,
	})
	if err != nil {
		return result, err
	}
	if code != http.StatusOK {
		return result, fmt.Errorf("apply Group (%d): %s", code, apiMessage(body))
	}
	if err := strictjson.Decode(body, &result); err != nil || result.Group != input.Group || result.Version <= 0 {
		return nodegroup.Result{}, fmt.Errorf("invalid Group apply response")
	}
	return result, nil
}
