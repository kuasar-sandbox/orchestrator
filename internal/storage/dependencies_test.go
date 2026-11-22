package storage_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Enforce the extension boundary even in a pure-Go build that excludes FDB.
func TestStorageDriversDoNotDependOnBusinessPackages(t *testing.T) {
	for _, dir := range []string{"sqlengine", filepath.Join("..", "fdbdriver"), "record", "observe"} {
		err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			const root = "github.com/kuasar-sandbox/orchestrator/internal/"
			for _, imported := range parsed.Imports {
				name, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return err
				}
				if strings.HasPrefix(name, root) && name != root+"storage" && !strings.HasPrefix(name, root+"storage/") && name != root+"observebudget" {
					t.Errorf("generic storage package %s imports %s", path, name)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
