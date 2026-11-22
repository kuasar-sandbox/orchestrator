package metadata

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	SecretInline = "inline"
	SecretRef    = "ref"
)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidRecord, fmt.Sprintf(format, args...))
}
func ValidFingerprint(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func ValidSandboxIdentity(sandboxID, nodeSandboxID string, generation uint64) bool {
	return types.ValidLocalSandboxID(sandboxID) && types.ValidLocalSandboxID(nodeSandboxID) && nodeSandboxID == sandboxID+"-g"+strconv.FormatUint(generation, 10)
}
func ValidateSandbox(r SandboxRecord) error {
	if r.Group == "" || r.RouteKey == "" || !types.Profile(r.Profile).Valid() || !ValidSandboxIdentity(r.SandboxID, r.NodeSandboxID, r.SandboxGeneration) || r.NextSandboxGeneration <= r.SandboxGeneration {
		return invalid("invalid sandbox identity, profile or generation")
	}
	return nil
}
func ValidateSandboxRef(r NodeSandboxRef) error {
	if r.Group == "" || r.RouteKey == "" || !types.Profile(r.Profile).Valid() || !ValidSandboxIdentity(r.SandboxID, r.NodeSandboxID, r.SandboxGeneration) || !ValidFingerprint(r.APISecretFingerprint) {
		return invalid("invalid sandbox ownership")
	}
	return nil
}
func SandboxRef(r SandboxRecord) NodeSandboxRef {
	return NodeSandboxRef{Group: r.Group, RouteKey: r.RouteKey, SandboxID: r.SandboxID, NodeSandboxID: r.NodeSandboxID, SandboxGeneration: r.SandboxGeneration, Profile: r.Profile, APISecretFingerprint: r.APISecretFingerprint}
}
func ValidateBuild(r BuildRecord) error {
	if r.Group == "" || r.BuildID == "" || r.NodeID == "" || !r.Profile.Valid() || !ValidFingerprint(r.APISecretFingerprint) {
		return invalid("invalid build identity, profile or credentials")
	}
	return nil
}
func NormalizeKeyPair(p NodeKeyPair) (NodeKeyPair, error) {
	if !ValidFingerprint(p.APISecretFingerprint) || !ValidFingerprint(p.ManifestKeyFingerprint) {
		return NodeKeyPair{}, invalid("invalid key pair fingerprints")
	}
	normalize := func(typ, value, ref, fp string) (string, error) {
		if typ == "" && value != "" && ref == "" {
			typ = SecretInline
		}
		switch typ {
		case SecretInline:
			if !ValidFingerprint(value) || ref != "" {
				return "", invalid("invalid inline secret")
			}
			b, _ := hex.DecodeString(value)
			sum := sha256.Sum256(b)
			if hex.EncodeToString(sum[:]) != fp {
				return "", invalid("inline secret fingerprint mismatch")
			}
		case SecretRef:
			if ref == "" || value != "" {
				return "", invalid("invalid secret reference")
			}
		default:
			return "", invalid("unsupported secret carrier")
		}
		return typ, nil
	}
	var err error
	p.APISecretType, err = normalize(p.APISecretType, p.APISecret, p.APISecretRef, p.APISecretFingerprint)
	if err != nil {
		return NodeKeyPair{}, err
	}
	p.ManifestKeyType, err = normalize(p.ManifestKeyType, p.ManifestKey, p.ManifestKeyRef, p.ManifestKeyFingerprint)
	return p, err
}
func SameKeyPairMaterial(a, b NodeKeyPair) bool {
	return a.APISecretFingerprint == b.APISecretFingerprint && a.APISecretType == b.APISecretType && hmac.Equal([]byte(a.APISecret), []byte(b.APISecret)) && hmac.Equal([]byte(a.APISecretRef), []byte(b.APISecretRef)) && a.ManifestKeyFingerprint == b.ManifestKeyFingerprint && a.ManifestKeyType == b.ManifestKeyType && hmac.Equal([]byte(a.ManifestKey), []byte(b.ManifestKey)) && hmac.Equal([]byte(a.ManifestKeyRef), []byte(b.ManifestKeyRef))
}
func SameBuildTarget(a, b *types.BuildTarget) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// MergeBuildAccepted computes a metadata-only ACK decision. A true changed value
// must be committed against current.Version before it can trigger business work.
func MergeBuildAccepted(current Versioned[BuildRecord], nodeID, templateID string, target *types.BuildTarget) (BuildRecord, bool, error) {
	if !current.Exists || current.Value.NodeID != nodeID || current.Value.TemplateID != templateID {
		return BuildRecord{}, false, ErrConflict
	}
	if target != nil {
		if err := target.Validate(); err != nil {
			return BuildRecord{}, false, invalid("invalid accepted target: %v", err)
		}
	}
	next := current.Value
	if next.RegistrationTargetSet {
		if !SameBuildTarget(next.RegistrationTarget, target) {
			return BuildRecord{}, false, ErrConflict
		}
		if next.State != BuildStarting {
			return next, false, nil
		}
	}
	if target != nil {
		v := *target
		next.RegistrationTarget = &v
	} else {
		next.RegistrationTarget = nil
	}
	next.RegistrationTargetSet = true
	next.RegistrationImageRepo = ""
	next.RegistrationRegistryAuth = ""
	if next.State == BuildStarting {
		next.State = BuildRegistered
	}
	return next, true, nil
}

// MergeRouteReport has no side effects. ReportWritten means the returned value
// needs a conditional commit; its Version is still the original expected token.
func MergeRouteReport(current Versioned[SandboxRecord], nodeID string, ref NodeSandboxRef, event routesync.RouteEntry, nowUnix int64) (RouteReportResult, error) {
	out := RouteReportResult{Disposition: ReportIgnored, Record: current}
	if err := ValidateSandboxRef(ref); err != nil {
		return out, err
	}
	if event.SandboxID == "" || event.SandboxID != ref.NodeSandboxID || event.Profile != ref.Profile {
		out.Disposition = ReportOrphan
		return out, nil
	}
	if event.State == routesync.StateStarting {
		return out, nil
	}
	var state SandboxState
	switch event.State {
	case routesync.StateRunning:
		state = StateReady
	case routesync.StatePaused:
		state = StatePaused
	case routesync.StatePausing:
		state = StatePausing
	case routesync.StateDead:
		state = StateDead
	case routesync.StateDeleting:
		state = StateDeleting
	default:
		return out, nil
	}
	cur := current.Value
	if !current.Exists || cur.SandboxID != ref.SandboxID || cur.NodeSandboxID != event.SandboxID || cur.SandboxGeneration != ref.SandboxGeneration || (cur.NodeID != "" && cur.NodeID != nodeID) || cur.Profile != ref.Profile || cur.APISecretFingerprint != ref.APISecretFingerprint {
		out.Disposition = ReportOrphan
		return out, nil
	}
	if err := validateRouteCredentials(event, ref.APISecretFingerprint, ref.SandboxID); err != nil {
		out.Reason = err.Error()
		return out, nil
	}
	if cur.InstanceID != "" && (event.InstanceID != cur.InstanceID || event.PressureVersion < cur.StateVersion) {
		return out, nil
	}
	if cur.InstanceID != "" && event.PressureVersion == cur.StateVersion && (cur.State == StateDead || cur.State == StateDeleting) && state != cur.State {
		return out, nil
	}
	next := SandboxRecord{Group: ref.Group, RouteKey: ref.RouteKey, SandboxID: ref.SandboxID, NodeSandboxID: ref.NodeSandboxID, SandboxGeneration: ref.SandboxGeneration, NextSandboxGeneration: cur.NextSandboxGeneration, State: state, NodeID: nodeID, Profile: ref.Profile, APISecretFingerprint: ref.APISecretFingerprint, InstanceID: event.InstanceID, StateVersion: event.PressureVersion, ArtifactLocation: event.ArtifactLocation, TemplateID: event.TemplateID, ManifestKeyFingerprint: event.ManifestKeyFingerprint, StableID: event.StableID, APISecret: event.APISecret, ServiceSecret: event.ServiceSecret, EnvdAccessToken: event.EnvdAccessToken, TrafficAccessToken: event.TrafficAccessToken, ForwardAccessToken: event.ForwardAccessToken, LastActive: nowUnix, TargetPort: cur.TargetPort, MaxCheckpoints: cur.MaxCheckpoints, AutoPauseMemory: cur.AutoPauseMemory, AutoReset: cur.AutoReset, IdleTimeout: types.CloneIdleTimeout(event.IdleTimeout)}
	if next.TemplateID == "" {
		next.TemplateID = cur.TemplateID
	}
	hasCredentials := cur.ManifestKeyFingerprint != "" || cur.StableID != "" || cur.APISecret != "" || cur.ServiceSecret != "" || cur.EnvdAccessToken != "" || cur.TrafficAccessToken != "" || cur.ForwardAccessToken != ""
	sameCredentials := next.APISecretFingerprint == cur.APISecretFingerprint && next.ManifestKeyFingerprint == cur.ManifestKeyFingerprint && next.StableID == cur.StableID && hmac.Equal([]byte(next.APISecret), []byte(cur.APISecret)) && hmac.Equal([]byte(next.ServiceSecret), []byte(cur.ServiceSecret)) && hmac.Equal([]byte(next.EnvdAccessToken), []byte(cur.EnvdAccessToken)) && hmac.Equal([]byte(next.TrafficAccessToken), []byte(cur.TrafficAccessToken)) && hmac.Equal([]byte(next.ForwardAccessToken), []byte(cur.ForwardAccessToken))
	if hasCredentials && !sameCredentials {
		out.Reason = "route credential rebinding"
		return out, nil
	}
	if err := ValidateSandbox(next); err != nil {
		return out, err
	}
	out.Disposition = ReportWritten
	out.Record = Versioned[SandboxRecord]{Exists: true, Version: current.Version, Value: next}
	return out, nil
}
func validateRouteCredentials(e routesync.RouteEntry, apiFingerprint, stableID string) error {
	if !ValidFingerprint(e.APISecret) {
		return invalid("invalid route API secret")
	}
	raw, _ := hex.DecodeString(e.APISecret)
	sum := sha256.Sum256(raw)
	fp := hex.EncodeToString(sum[:])
	if fp != e.APISecretFingerprint || fp != apiFingerprint {
		return invalid("route API secret binding")
	}
	if !ValidFingerprint(e.ManifestKeyFingerprint) || e.StableID == "" || !utf8.ValidString(e.StableID) || (stableID != "" && e.StableID != stableID) {
		return invalid("route credential identity")
	}
	if err := keys.VerifyForwardAccessToken(e.ForwardAccessToken, e.ServiceSecret, e.StableID); err != nil {
		return invalid("route forward credential")
	}
	if !sandboxcfg.ValidE2BAccessToken(e.EnvdAccessToken) || !sandboxcfg.ValidE2BAccessToken(e.TrafficAccessToken) {
		return invalid("route e2b credential")
	}
	switch types.Profile(e.Profile) {
	case types.ProfileE2B:
		if e.EnvdAccessToken == "" || e.TrafficAccessToken == "" {
			return invalid("missing route e2b credentials")
		}
	case types.ProfileBare:
		if e.EnvdAccessToken != "" || e.TrafficAccessToken != "" {
			return invalid("bare route e2b credentials")
		}
	default:
		return invalid("route profile")
	}
	return nil
}
