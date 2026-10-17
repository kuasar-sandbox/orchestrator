package proxyapp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"golang.org/x/sys/unix"
)

const continuationEnvironment = "KUASAR_INTERNAL_PROXY_CONTINUATION_FD"
const continuationMagic = "kuasar-proxy-inplace-restart"
const continuationVersion = 1

// ServiceInput is the immutable declarative input supplied to every Configure
// invocation. Effective Config and B's derived paths never replace this input.
type ServiceInput struct {
	Config     json.RawMessage `json:"config"`
	Executable string          `json:"executable"`
	Native     bool            `json:"native"`
	Signals    bool            `json:"signals"`
}

type continuationFiles struct {
	Shm              int `json:"shm"`
	Stop             int `json:"stop"`
	Data             int `json:"data"`
	MMDS             int `json:"mmds"`
	Stats            int `json:"stats"`
	Metrics          int `json:"metrics"`
	CandidateMetrics int `json:"candidate_metrics"`
	Target           int `json:"target"`
	Control          int `json:"control"`
}

type continuationEnvelope struct {
	Magic                      string                           `json:"magic"`
	Version                    int                              `json:"version"`
	Mode                       string                           `json:"mode"`
	Input                      ServiceInput                     `json:"input"`
	InputDigest                string                           `json:"input_digest"`
	Baseline                   json.RawMessage                  `json:"baseline"`
	BootID                     string                           `json:"boot_id"`
	InstanceID                 string                           `json:"instance_id"`
	Operation                  RestartStatus                    `json:"operation"`
	TargetIdentity             componentexec.ExecutableIdentity `json:"target_identity"`
	TargetDigest               string                           `json:"target_digest"`
	DataExpression             string                           `json:"data_expression"`
	MMDSExpression             string                           `json:"mmds_expression"`
	StatsPath                  string                           `json:"stats_path"`
	ShmPath                    string                           `json:"shm_path"`
	StatsIdentity              pathIdentity                     `json:"stats_identity"`
	ShmIdentity                pathIdentity                     `json:"shm_identity"`
	MetricsExpression          string                           `json:"metrics_expression"`
	CandidateStatsPath         string                           `json:"candidate_stats_path"`
	CandidateShmPath           string                           `json:"candidate_shm_path"`
	CandidateStatsIdentity     pathIdentity                     `json:"candidate_stats_identity"`
	CandidateShmIdentity       pathIdentity                     `json:"candidate_shm_identity"`
	CandidateMetricsExpression string                           `json:"candidate_metrics_expression"`
	CandidatePID               int                              `json:"candidate_pid"`
	CandidateBootID            string                           `json:"candidate_boot_id"`
	Files                      continuationFiles                `json:"files"`
}

// Continuation owns only the descriptors in a verified, sealed private
// handoff. Its Close must run after the service coordinator relinquishes them.
type Continuation struct {
	envelope continuationEnvelope
	files    map[int]*os.File
}

func ContinuationPresent() bool { _, ok := os.LookupEnv(continuationEnvironment); return ok }

func (c *Continuation) Input() ServiceInput {
	input := c.envelope.Input
	input.Config = append(json.RawMessage(nil), input.Config...)
	return input
}

func (c *Continuation) Close() {
	if c == nil {
		return
	}
	for _, file := range c.files {
		_ = file.Close()
	}
	c.files = nil
}

func (c *Continuation) take(fd int) *os.File {
	file := c.files[fd]
	delete(c.files, fd)
	return file
}

func continuationDescriptors(files continuationFiles) []int {
	return []int{files.Shm, files.Stop, files.Data, files.MMDS, files.Stats, files.Metrics, files.CandidateMetrics, files.Target, files.Control}
}

func encodeContinuation(envelope continuationEnvelope) (*os.File, error) {
	envelope.Magic = continuationMagic
	envelope.Version = continuationVersion
	digest := sha256.Sum256(envelope.Input.Config)
	envelope.InputDigest = hex.EncodeToString(digest[:])
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxWorkerBootstrapBytes*2 {
		return nil, fmt.Errorf("proxy continuation exceeds size limit")
	}
	return newSealedRuntimeFile("kuasar-proxy-continuation", payload)
}

// ReceiveContinuation consumes the private envelope before CLI/config handling.
// No business initialization is performed here. The caller next restores child
// supervision and stop handling before running any user Configure/BindRuntime.
func ReceiveContinuation() (_ *Continuation, returnErr error) {
	rawFD, ok := os.LookupEnv(continuationEnvironment)
	_ = os.Unsetenv(continuationEnvironment)
	if !ok {
		return nil, fmt.Errorf("proxy continuation is missing")
	}
	fd, err := strconv.Atoi(rawFD)
	if err != nil || fd < 3 || strconv.Itoa(fd) != rawFD {
		return nil, fmt.Errorf("proxy continuation descriptor is invalid")
	}
	file := os.NewFile(uintptr(fd), "kuasar-proxy-continuation")
	if file == nil {
		return nil, fmt.Errorf("proxy continuation descriptor is invalid")
	}
	defer file.Close()
	seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	want := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if err != nil || seals&want != want {
		return nil, fmt.Errorf("proxy continuation is not a sealed file")
	}
	payload, err := io.ReadAll(io.LimitReader(file, 2*maxWorkerBootstrapBytes+1))
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || len(payload) > 2*maxWorkerBootstrapBytes {
		return nil, fmt.Errorf("proxy continuation has invalid size")
	}
	var envelope continuationEnvelope
	if err := strictjson.Decode(payload, &envelope); err != nil {
		return nil, err
	}
	if envelope.Magic != continuationMagic || envelope.Version != continuationVersion {
		return nil, fmt.Errorf("%w: continuation version", ErrIncompatible)
	}
	if envelope.Mode != "candidate" && envelope.Mode != "resume" {
		return nil, fmt.Errorf("proxy continuation has invalid mode")
	}
	if envelope.BootID == "" || envelope.InstanceID == "" || envelope.Operation.ID == "" || !filepath.IsAbs(envelope.Input.Executable) {
		return nil, fmt.Errorf("proxy continuation has invalid identity or input")
	}
	digest := sha256.Sum256(envelope.Input.Config)
	if envelope.InputDigest != hex.EncodeToString(digest[:]) {
		return nil, fmt.Errorf("proxy continuation input digest mismatch")
	}
	if envelope.TargetIdentity.Inode == 0 || len(envelope.TargetDigest) != 64 {
		return nil, fmt.Errorf("proxy continuation target identity is invalid")
	}
	if envelope.Files.Data < 3 || envelope.Files.Target < 3 || envelope.Files.Control < 3 {
		return nil, fmt.Errorf("proxy continuation required descriptors are missing")
	}
	seen := map[int]bool{fd: true}
	for _, descriptor := range continuationDescriptors(envelope.Files) {
		if descriptor == -1 {
			continue
		}
		if descriptor < 3 || descriptor > 1<<20 || seen[descriptor] {
			return nil, fmt.Errorf("proxy continuation has invalid or duplicate descriptor")
		}
		seen[descriptor] = true
	}
	continuation := &Continuation{envelope: envelope, files: make(map[int]*os.File)}
	defer func() {
		if returnErr != nil {
			continuation.Close()
		}
	}()
	for _, descriptor := range continuationDescriptors(envelope.Files) {
		if descriptor < 0 {
			continue
		}
		owned, err := inheritedFile(descriptor, "proxy-continuation-resource")
		if err != nil {
			return nil, err
		}
		continuation.files[descriptor] = owned
	}
	return continuation, nil
}
