package prefetch

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ErrUnsupportedReference reports a reference whose shape no resolver branch
// understands. The API maps it to a request error (client fault), not a
// best-effort warm failure.
var ErrUnsupportedReference = errors.New("prefetch: unsupported reference")

// resolver turns a PrefetchReq.Reference into the concrete files to warm. It
// branches on the reference shape; today only the e2b-snp-... template-id form
// is implemented.
type resolver struct {
	cfg config.CheckpointConfig
}

// target is the resolved artifact list for one reference. refKey is a stable
// dedup key derived from the reference.
type target struct {
	refKey string
	paths  []string
}

// resolve dispatches on the reference shape.
func (r resolver) resolve(ctx context.Context, reference string) (target, error) {
	if !strings.HasPrefix(reference, "e2b-") {
		return target{}, ErrUnsupportedReference
	}
	return r.resolveTemplateID(reference)
}

// resolveTemplateID handles the e2b-snp-<base64url(portable-ref)> form.
func (r resolver) resolveTemplateID(reference string) (target, error) {
	tid, err := types.ParseTemplateID(reference)
	if err != nil {
		return target{}, fmt.Errorf("%w: %v", ErrUnsupportedReference, err)
	}
	if tid.Kind != types.KindSnp {
		return target{}, fmt.Errorf("%w: template kind %s is not a snapshot", ErrUnsupportedReference, tid.Kind)
	}
	ref, err := manifest.ParseRef(tid.Ref)
	if err != nil {
		return target{}, fmt.Errorf("%w: parse portable ref: %v", ErrUnsupportedReference, err)
	}
	if ref.Scheme != manifest.RefSchemeFile || ref.Location == "" {
		return target{}, fmt.Errorf("%w: snapshot template must be a located-file reference", ErrUnsupportedReference)
	}
	uri, err := r.cfg.RefLocationURI(ref.Location)
	if err != nil {
		return target{}, err
	}
	dir, err := filePathFromURI(uri)
	if err != nil {
		return target{}, err
	}
	paths, err := globBundlePaths(dir)
	if err != nil {
		return target{}, err
	}
	if len(paths) == 0 {
		return target{}, fmt.Errorf("prefetch: no snapshot artifacts found under %s", dir)
	}
	return target{refKey: ref.Location, paths: paths}, nil
}

// globBundlePaths returns the memory snapshot and disk overlay artifacts in a
// snapshot bundle directory, i.e. every *.snapshot and *.overlay file. Missing
// individual artifacts are non-fatal (cold restore just falls back on demand).
func globBundlePaths(dir string) ([]string, error) {
	var out []string
	for _, pattern := range []string{"*.snapshot", "*.overlay"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, fmt.Errorf("prefetch: glob %s: %w", pattern, err)
		}
		out = append(out, matches...)
	}
	return out, nil
}

// filePathFromURI extracts a filesystem path from a file:// URI (the form
// returned by CheckpointConfig.RefLocationURI).
func filePathFromURI(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("prefetch: parse uri %q: %w", raw, err)
	}
	if u.Scheme != "file" || u.Path == "" {
		return "", fmt.Errorf("prefetch: uri %q is not an absolute file path", raw)
	}
	fi, err := os.Stat(u.Path)
	if err != nil {
		return "", fmt.Errorf("prefetch: stat %s: %w", u.Path, err)
	}
	if !fi.IsDir() {
		return "", fmt.Errorf("prefetch: %s is not a directory", u.Path)
	}
	return u.Path, nil
}
