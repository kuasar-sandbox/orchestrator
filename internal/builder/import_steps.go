package builder

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// --- phase A: import -------------------------------------------------------

func (p *buildPipeline) phaseImport() error {
	s := p.spec
	sb, err := p.startSandbox("a", p.importYAML(), nil)
	if err != nil {
		return err
	}
	defer sb.teardown()
	bootCtx, cancelBoot := context.WithTimeout(p.ctx, 60*time.Second)
	defer cancelBoot()
	if err := sb.waitRuntimeReady(bootCtx); err != nil {
		return err
	}
	cancelBoot()

	var refSupported bool
	var refSubject string
	importRef := s.FromImage
	if s.ImportReferer.Enabled {
		lookup, err := p.lookupImportReferer(sb)
		if err == nil {
			importRef = importSourceReference(s.FromImage, lookup)
		}
		switch {
		case err != nil:
			if !s.ImportReferer.Fallback {
				return fmt.Errorf("referer lookup: %w", err)
			}
			p.progress("import: referer lookup failed (%v); falling back to pull+flatten", err)
		case !lookup.Supported:
			if !s.ImportReferer.Fallback {
				return fmt.Errorf("referer lookup: registry does not support OCI referrers")
			}
			p.progress("import: registry does not support OCI referrers; falling back to pull+flatten")
		case lookup.Hit:
			if err := p.useImportRefererHit(lookup.ManifestID); err != nil {
				return err
			}
			p.progress("import: referer hit %s", lookup.ManifestID)
			return nil
		default:
			refSupported = true
			refSubject = lookup.Subject
			p.progress("import: referer miss for %s", refSubject)
		}
	}

	// No --no-progress: flatten-ctl's pull/flatten progress goes to the guest
	// command stderr, which sandbox-ctl streams to journald=build (stdout is
	// the artifact). So the SDK sees `pull: N/M layers`, `flatten: …` live.
	args := []string{"export", "--tmpdir", "/pull"}
	if s.Insecure {
		args = append(args, "--insecure")
	}
	if s.Platform != "" {
		args = append(args, "--platform", s.Platform)
	}
	args = append(args, "--output", "-", importRef)

	imagePath := filepath.Join(s.Workdir, "image.img")
	ctx, cancel := context.WithTimeout(p.ctx, time.Duration(s.Timeouts.PullSec)*time.Second)
	defer cancel()
	p.progress("import: pulling + flattening %s", importRef)
	if err := sb.exec(ctx, execOpts{env: p.tenantEnv(), stdoutTo: imagePath, stderrTo: "journald=" + buildTag},
		append([]string{guestFlatten}, args...)...); err != nil {
		return err
	}
	if st, err := os.Stat(imagePath); err != nil || st.Size() == 0 {
		return fmt.Errorf("no image artifact produced")
	}
	if err := p.useLocalImage(imagePath); err != nil {
		return err
	}
	p.progress("import: image artifact ready")
	if refSupported && s.ImportReferer.Writeback {
		key, err := p.uploadImage()
		if err != nil {
			return fmt.Errorf("upload referer base image: %w", err)
		}
		p.baseRef = "manifest://" + key
		if err := p.writeImportReferer(sb, refSubject, key); err != nil {
			return fmt.Errorf("referer writeback: %w", err)
		}
		p.progress("import: referer writeback complete")
	}
	return nil
}

// useLocalImage records a freshly produced plaintext tarstream as the current
// complete base image. The explicit digest qualifier lets downstream
// sandbox-ctl consumers validate an artifact whose staging filename is not its
// content identity.
func (p *buildPipeline) useLocalImage(path string) error {
	stream, err := fetch.OpenTarStream(path)
	if err != nil {
		return fmt.Errorf("local image artifact: %w", err)
	}
	digester, ok := stream.(tarstream.Digester)
	if !ok {
		_ = stream.Close()
		return fmt.Errorf("local image artifact has no declared digest")
	}
	scheme, digest := digester.Digest()
	if err := stream.Close(); err != nil {
		return fmt.Errorf("close local image artifact: %w", err)
	}
	if scheme != tarstream.DigestSchemeSHA256 {
		return fmt.Errorf("local image artifact has unexpected digest scheme %q", scheme)
	}
	ref := manifest.Ref{
		Scheme:       manifest.RefSchemeFile,
		Path:         path,
		DigestScheme: scheme,
		Digest:       digest,
	}
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("local image artifact identity: %w", err)
	}

	p.imagePath = path
	p.baseImageRef = ""
	p.baseRef = ref.String()
	p.overlayBase = ""
	p.overlayBaseFromRefs = nil
	return nil
}

type importRefererLookup struct {
	Supported  bool   `json:"supported"`
	Subject    string `json:"subject"`
	Hit        bool   `json:"hit"`
	ManifestID string `json:"manifest_id"`
}

func importSourceReference(original string, lookup importRefererLookup) string {
	if lookup.Supported {
		return lookup.Subject
	}
	return original
}

func decodeImportRefererLookup(data []byte) (importRefererLookup, error) {
	var out importRefererLookup
	if err := json.Unmarshal(data, &out); err != nil {
		return importRefererLookup{}, fmt.Errorf("parse lookup result: %w", err)
	}
	if out.Supported {
		if _, err := name.NewDigest(out.Subject); err != nil {
			return importRefererLookup{}, fmt.Errorf("lookup result subject is not a digest: %w", err)
		}
	}
	return out, nil
}

func (p *buildPipeline) lookupImportReferer(sb *phaseSandbox) (importRefererLookup, error) {
	s := p.spec
	if s.ImportReferer.Owner == "" {
		return importRefererLookup{}, fmt.Errorf("owner token is empty")
	}
	outPath := filepath.Join(s.Workdir, "referer.lookup.json")
	args := []string{"referer", "lookup", "--json", "--owner", s.ImportReferer.Owner}
	if s.Insecure {
		args = append(args, "--insecure")
	}
	if s.Platform != "" {
		args = append(args, "--platform", s.Platform)
	}
	args = append(args, s.FromImage)
	ctx, cancel := context.WithTimeout(p.ctx, time.Duration(s.Timeouts.PullSec)*time.Second)
	defer cancel()
	p.progress("import: checking image referer")
	if err := sb.exec(ctx, execOpts{env: p.tenantEnv(), stdoutTo: outPath, stderrTo: "journald=" + buildTag},
		append([]string{guestFlatten}, args...)...); err != nil {
		return importRefererLookup{}, err
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		return importRefererLookup{}, err
	}
	return decodeImportRefererLookup(data)
}

func (p *buildPipeline) useImportRefererHit(id string) error {
	if !validManifestKey(id) {
		return fmt.Errorf("referer manifest id %q is not a 64-hex key", id)
	}
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.FlattenCtl,
		"info", "--json", "--manifest-config", p.spec.Paths.ManifestConfig, "manifest://"+id)
	if err != nil {
		return fmt.Errorf("validate referer manifest %s: %w (%s)", id, err, firstLine(out))
	}
	p.baseImageRef = "manifest://" + id
	p.baseRef = p.baseImageRef
	p.imagePath = ""
	p.overlayBase = ""
	return nil
}

func (p *buildPipeline) writeImportReferer(sb *phaseSandbox, subject, manifestID string) error {
	s := p.spec
	if subject == "" {
		return fmt.Errorf("subject is empty")
	}
	args := []string{"referer", "put", "--owner", s.ImportReferer.Owner, "--manifest-id", manifestID}
	if s.ImportReferer.Validity != "" {
		args = append(args, "--validity", s.ImportReferer.Validity)
	}
	if s.Insecure {
		args = append(args, "--insecure")
	}
	if s.Platform != "" {
		args = append(args, "--platform", s.Platform)
	}
	args = append(args, subject)
	ctx, cancel := context.WithTimeout(p.ctx, time.Duration(s.Timeouts.PullSec)*time.Second)
	defer cancel()
	return sb.exec(ctx, execOpts{env: p.tenantEnv(), stderrTo: "journald=" + buildTag},
		append([]string{guestFlatten}, args...)...)
}

func validManifestKey(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// tenantEnv is the FLATTEN_* (registry credential) subset of the spec env —
// the only env that ever enters a guest.
func (p *buildPipeline) tenantEnv() []string {
	var out []string
	for k, v := range p.spec.Env {
		if strings.HasPrefix(k, "FLATTEN_") {
			out = append(out, k+"="+v)
		}
	}
	return out
}

// --- phase B: steps --------------------------------------------------------

// stepCtx is the Dockerfile-ish build context the host accumulates,
// seeded from the base image's runtime config — RUN must see the image's
// ENV/WORKDIR/USER exactly as `docker build` would.
type stepCtx struct {
	env     map[string]string // ENV: persisted into the image config
	args    map[string]string // ARG: substitution only
	workdir string
	user    string
}

func stepCtxFrom(base map[string]any) *stepCtx {
	c := &stepCtx{env: map[string]string{}, args: map[string]string{}}
	if envs, ok := base["Env"].([]any); ok {
		for _, e := range envs {
			if s, ok := e.(string); ok {
				if k, v, ok := strings.Cut(s, "="); ok {
					c.env[k] = v
				}
			}
		}
	}
	if wd, ok := base["WorkingDir"].(string); ok {
		c.workdir = wd
	}
	if u, ok := base["User"].(string); ok {
		c.user = u
	}
	return c
}

func (p *buildPipeline) phaseSteps() error {
	s := p.spec
	baseCfg, err := p.readBaseRuntimeConfig()
	if err != nil {
		return err
	}
	ctxv := stepCtxFrom(baseCfg)

	envdUDS := filepath.Join(s.Workdir, "envd-steps.sock")
	sb, err := p.startSandbox("b", p.stepsYAML(),
		[]string{envdUDS + ":127.0.0.1:49983"})
	if err != nil {
		return err
	}
	defer sb.teardown()
	bootCtx, cancelBoot := context.WithTimeout(p.ctx, 90*time.Second)
	defer cancelBoot()
	if err := sb.waitRuntimeReady(bootCtx); err != nil {
		return err
	}
	if err := p.waitEnvd(bootCtx, envdUDS); err != nil {
		return err
	}
	cancelBoot()

	// RUN steps go through envd — the e2b exec channel (the step sandbox's
	// envd is a plain build tool: -isnotfc, never /init-armed, no token).
	// COPY instead streams its context tar through sandbox-ctl exec into
	// flatten-ctl (a platform filesystem op, not an e2b process) — sb carries
	// that channel.
	sess := &envdExec{uds: envdUDS, log: p.log, out: p.out}
	for i, st := range s.Steps {
		if err := p.applyStep(sb, sess, ctxv, i, st); err != nil {
			return err
		}
	}

	// Export the built rootfs: tmpdir/output live under a self-bind
	// mountpoint so --skip-mounts excludes them (and the toolchain mount).
	if err := sb.exec(p.ctx, execOpts{}, guestFlatten, "mountpoint", "/.kuasar-build"); err != nil {
		return fmt.Errorf("mountpoint: %w", err)
	}
	cfgJSON, err := mergedRuntimeConfig(baseCfg, ctxv)
	if err != nil {
		return err
	}
	cfgPath := filepath.Join(s.Workdir, "config.json")
	if err := os.WriteFile(cfgPath, cfgJSON, 0o600); err != nil {
		return err
	}
	if err := sb.exec(p.ctx, execOpts{stdinFrom: cfgPath},
		"/bin/sh", "-c", "cat > /.kuasar-build/config.json"); err != nil {
		return fmt.Errorf("write runtime config: %w", err)
	}
	newImg := filepath.Join(s.Workdir, "image.new.img")
	ctx, cancel := context.WithTimeout(p.ctx, time.Duration(s.Timeouts.PullSec)*time.Second)
	defer cancel()
	p.progress("steps: exporting rootfs")
	if err := sb.exec(ctx, execOpts{stdoutTo: newImg, stderrTo: "journald=" + buildTag},
		guestFlatten, "export", "--skip-mounts",
		"--runtime-config", "/.kuasar-build/config.json",
		"--tmpdir", "/.kuasar-build", "--output", "-", "/"); err != nil {
		return err
	}
	imagePath := filepath.Join(s.Workdir, "image.img")
	if err := os.Rename(newImg, imagePath); err != nil {
		return err
	}
	return p.useLocalImage(imagePath)
}

// applyStep executes one build step. RUN goes to the guest through envd
// (the e2b exec channel: /bin/bash -l -c with the accumulated context;
// docker defaults — root, "/" — when the context has none); COPY streams
// its tar through sandbox-ctl exec into flatten-ctl (sb); the rest transform
// the host-side context.
func (p *buildPipeline) applyStep(sb *phaseSandbox, sess *envdExec, c *stepCtx, i int, st configsock.BuildStep) error {
	sub := func(v string) string { // ARG/ENV ${k} substitution
		for k, val := range c.args {
			v = strings.ReplaceAll(v, "${"+k+"}", val)
			v = strings.ReplaceAll(v, "$"+k, val)
		}
		return v
	}
	kv := func() (string, string) { // "K=V" or [K, V] arg shapes
		if len(st.Args) >= 2 {
			return st.Args[0], sub(st.Args[1])
		}
		if len(st.Args) == 1 {
			if k, v, ok := strings.Cut(st.Args[0], "="); ok {
				return k, sub(v)
			}
			return st.Args[0], ""
		}
		return "", ""
	}
	switch strings.ToUpper(st.Type) {
	case "RUN":
		cmd := sub(strings.Join(st.Args, " "))
		p.progress("step %d: RUN %s", i, cmd)
		user, cwd := c.user, c.workdir
		if user == "" {
			user = "root"
		}
		if cwd == "" {
			cwd = "/"
		}
		stepBudget := time.Duration(p.spec.Timeouts.StepSec) * time.Second
		if err := sess.run(p.ctx, user, cwd, c.env, cmd, stepBudget, false); err != nil {
			return fmt.Errorf("step %d (RUN %s): %w", i, cmd, err)
		}
	case "ENV":
		k, v := kv()
		if k != "" {
			c.env[k] = v
			p.progress("step %d: ENV %s=%s", i, k, v)
		}
	case "ARG":
		k, v := kv()
		if k != "" {
			c.args[k] = v
			p.progress("step %d: ARG %s=%s", i, k, v)
		}
	case "WORKDIR":
		if len(st.Args) > 0 {
			c.workdir = sub(st.Args[0])
			p.progress("step %d: WORKDIR %s", i, c.workdir)
		}
	case "USER":
		if len(st.Args) > 0 {
			c.user = sub(st.Args[0])
			p.progress("step %d: USER %s", i, c.user)
		}
	case "COPY", "ADD":
		if err := p.applyCopy(sb, c, i, st, sub); err != nil {
			return fmt.Errorf("step %d (COPY): %w", i, err)
		}
	default:
		return fmt.Errorf("step %d: unsupported type %q", i, st.Type)
	}
	return nil
}

// applyCopy realizes a COPY/ADD step: fetch the context tar (presigned GET,
// platform plumbing — host-side, no guest), then stream it through
// sandbox-ctl exec into `flatten-ctl tar extract` (a filesystem op on any
// rootfs; no tar/gzip needed in the image). The accumulated USER is NOT the
// COPY owner — Docker COPY defaults to root unless --chown is given.
func (p *buildPipeline) applyCopy(sb *phaseSandbox, c *stepCtx, i int, st configsock.BuildStep, sub func(string) string) error {
	if len(st.Args) < 2 {
		return fmt.Errorf("needs <src> <dst>")
	}
	if st.FilesHash == "" || st.FilesURL == "" {
		return fmt.Errorf("no uploaded context (filesHash/url missing — files_storage configured?)")
	}
	rawTar, err := p.fetchCopyContext(st.FilesURL, st.FilesHash)
	if err != nil {
		return err
	}
	defer os.Remove(rawTar)

	entries, err := peekTarEntries(rawTar)
	if err != nil {
		return fmt.Errorf("read context tar: %w", err)
	}
	rule, err := copyRule(sub(st.Args[0]), sub(st.Args[1]), c.workdir, entries)
	if err != nil {
		return err
	}

	owner := "0:0" // Docker COPY default; --chown overrides
	if len(st.Args) >= 3 && sub(st.Args[2]) != "" {
		owner = sub(st.Args[2])
	}
	args := []string{guestFlatten, "tar", "extract", "--dense", "--chown", owner}
	if len(st.Args) >= 4 && sub(st.Args[3]) != "" {
		args = append(args, "--chmod", sub(st.Args[3]))
	}
	args = append(args, rule)

	p.progress("step %d: COPY %s -> %s (owner %s)", i, st.Args[0], st.Args[1], owner)
	ctx, cancel := context.WithTimeout(p.ctx, time.Duration(p.spec.Timeouts.StepSec)*time.Second)
	defer cancel()
	return sb.exec(ctx, execOpts{stdinFrom: rawTar, stderrTo: "journald=" + buildTag}, args...)
}

// fetchCopyContext downloads the gzipped context tar from the presigned GET
// URL and gunzips it to a workdir file (flatten-ctl tar extract takes a plain
// tar on stdin). The e2b SDK uploads w:gz; we strip the gzip host-side so the
// image needs no gzip.
func (p *buildPipeline) fetchCopyContext(url, hash string) (string, error) {
	ctx, cancel := context.WithTimeout(p.ctx, time.Duration(p.spec.Timeouts.PullSec)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch context: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("fetch context: status %d (%s)", resp.StatusCode, firstLine(body))
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return "", fmt.Errorf("context not gzip: %w", err)
	}
	defer gz.Close()
	out := filepath.Join(p.spec.Workdir, "copy-"+hash+".tar")
	f, err := os.Create(out)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, gz); err != nil {
		f.Close()
		os.Remove(out)
		return "", fmt.Errorf("gunzip context: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(out)
		return "", err
	}
	return out, nil
}

// peekTarEntries lists the tar's member names (slash paths, "./" and root
// dropped) so copyRule can tell a single file from a directory tree.
func peekTarEntries(tarPath string) ([]string, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		n := path.Clean(strings.TrimPrefix(h.Name, "./"))
		if n == "" || n == "." {
			continue
		}
		names = append(names, n)
	}
	return names, nil
}

// copyRule maps a COPY (src,dst) + the context tar's entries to a
// `flatten-ctl tar extract` rule, matching Docker/e2b semantics for the
// single-source forms the e2b SDK emits (arcnames rooted at the build
// context). dst resolves against the accumulated WORKDIR (default "/").
// Multi-source COPY is unsupported (e2b's executor only reads src+dst).
func copyRule(srcArg, dstArg, workdir string, entries []string) (string, error) {
	srcPrefix := normalizeCopySrc(srcArg)

	dstSlash := strings.HasSuffix(dstArg, "/") || dstArg == "." || dstArg == ".."
	dst := dstArg
	if !path.IsAbs(dst) {
		base := workdir
		if base == "" {
			base = "/"
		}
		dst = path.Join(base, dst)
	}
	dst = path.Clean(dst) // absolute, no trailing slash

	// Whole context (COPY . / COPY *): map the archive root under dst.
	if srcPrefix == "" {
		return ":" + dst + "/", nil
	}
	// Single file iff an entry equals srcPrefix and none nests under it.
	exact, under := false, false
	for _, e := range entries {
		if e == srcPrefix {
			exact = true
		}
		if strings.HasPrefix(e, srcPrefix+"/") {
			under = true
		}
	}
	if exact && !under {
		out := dst
		if dstSlash { // dst is a directory → keep the file's basename
			out = path.Join(dst, path.Base(srcPrefix))
		}
		return srcPrefix + ":" + out, nil
	}
	if !under {
		return "", fmt.Errorf("source %q not found in context", srcArg)
	}
	// Directory: strip the src prefix, place its contents under dst.
	return srcPrefix + "/:" + dst + "/", nil
}

// normalizeCopySrc strips a trailing glob and surrounding slashes/"./",
// yielding the tar path prefix to select ("" for the whole context).
func normalizeCopySrc(s string) string {
	if i := strings.IndexAny(s, "*?["); i >= 0 {
		s = s[:i] // drop the glob tail; the SDK already expanded matches into the tar
	}
	s = strings.TrimPrefix(s, "./")
	s = strings.Trim(s, "/")
	s = path.Clean(s)
	if s == "." || s == ".." {
		return ""
	}
	return s
}

// readBaseRuntimeConfig reads the base image's runtime config (through
// the local artifact or the manifest store). It both seeds the step
// context and is the base the exported config merges onto.
func (p *buildPipeline) readBaseRuntimeConfig() (map[string]any, error) {
	target := p.baseRef
	if p.imagePath != "" {
		target = p.imagePath
	} else {
		target = strings.TrimPrefix(target, "file://")
	}
	args := []string{"info", "--json"}
	if strings.HasPrefix(target, "manifest://") {
		args = append(args, "--manifest-config", p.spec.Paths.ManifestConfig)
	}
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.FlattenCtl, append(args, target)...)
	if err != nil {
		return nil, fmt.Errorf("read base runtime config: %w", err)
	}
	var info struct {
		Config map[string]any `json:"config"`
	}
	if err := json.Unmarshal(out, &info); err != nil {
		return nil, fmt.Errorf("parse base runtime config: %w", err)
	}
	if info.Config == nil {
		return map[string]any{}, nil
	}
	return info.Config, nil
}

// mergedRuntimeConfig overlays the accumulated context onto the base
// config for the exported image. The step context was seeded from the
// base, so its env is the full set; emit it sorted — map order would
// make the artifact (and its manifest key) nondeterministic.
func mergedRuntimeConfig(base map[string]any, c *stepCtx) ([]byte, error) {
	cfg := make(map[string]any, len(base))
	for k, v := range base {
		cfg[k] = v
	}
	if len(c.env) > 0 {
		keys := make([]string, 0, len(c.env))
		for k := range c.env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		envs := make([]string, 0, len(keys))
		for _, k := range keys {
			envs = append(envs, k+"="+c.env[k])
		}
		cfg["Env"] = envs
	}
	if c.workdir != "" {
		cfg["WorkingDir"] = c.workdir
	}
	if c.user != "" {
		cfg["User"] = c.user
	}
	return json.Marshal(cfg)
}
