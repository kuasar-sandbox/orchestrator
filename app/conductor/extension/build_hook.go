package extension

// BuildOperationKind identifies one hookable Build admission.
type BuildOperationKind string

const (
	BuildOperationRegister BuildOperationKind = "register"
	BuildOperationTrigger  BuildOperationKind = "trigger"
)

// BuildOperationOrigin identifies direct node API and canonical cluster
// registration paths. It does not add an Extension to cluster components.
type BuildOperationOrigin string

const (
	BuildOriginDirect  BuildOperationOrigin = "direct"
	BuildOriginCluster BuildOperationOrigin = "cluster"
)

// BuildOperation is an operation-specific mutable copy. Exactly one request
// field is non-nil. ID, Kind, Origin, and BuildID are core-owned envelope
// identity and modifications are rejected.
type BuildOperation struct {
	ID      string
	Kind    BuildOperationKind
	Origin  BuildOperationOrigin
	BuildID string

	Current *BuildView

	Register *BuildRegisterRequest
	Trigger  *BuildTriggerRequest
}

// BuildRegisterRequest is the mutable, credential-free registration
// definition. TemplateID is allocated by the core and cannot be changed.
type BuildRegisterRequest struct {
	TemplateID string
	Profile    Profile
	Kind       BuildKind
	Names      []string
	Aliases    []string
	Resources  BuildResources
	Metadata   map[string]string
	Builder    BuildOptions
}

// BuildResourcePatch is an optional trigger-time assertion. Nil leaves are not
// asserted against the immutable registration resources.
type BuildResourcePatch struct {
	CPU     *int64
	Memory  *int64
	Storage *int64
}

// BuildTriggerRequest is the mutable, credential-free Build work order.
// Registry credentials are resolved only after the final source is validated.
type BuildTriggerRequest struct {
	FromImage         string
	FromTemplate      string
	Steps             []BuildStep
	StartCommand      string
	ReadyCommand      string
	ResourceAssertion BuildResourcePatch
}
