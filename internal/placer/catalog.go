package placer

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/placement"
)

const (
	FinalCatalogSyncPath           = "/internal/placement/catalog"
	MaximumCatalogSyncRequestBytes = 1 << 20
	MaxCatalogSyncPageNodes        = 256
	maximumInstalledCatalogs       = 4
	maximumStagedCatalogs          = 4
	maximumStagedCatalogBytes      = 256 << 20
	PlanErrorCatalogUnavailable    = "CATALOG_UNAVAILABLE"
)

var ErrCatalogUnavailable = errors.New("placer: referenced Node Catalog is unavailable")

type CatalogSyncRequest struct {
	Reference placement.CatalogReference `json:"reference"`
	Start     uint32                     `json:"start"`
	Nodes     []placement.CatalogNode    `json:"nodes"`
	Final     bool                       `json:"final"`
}

func (r CatalogSyncRequest) Validate() error {
	if err := r.Reference.Validate(); err != nil {
		return err
	}
	if len(r.Nodes) > MaxCatalogSyncPageNodes || uint64(r.Start)+uint64(len(r.Nodes)) > uint64(r.Reference.NodeCount) {
		return errors.New("placer: invalid Node Catalog page bounds")
	}
	end := r.Start + uint32(len(r.Nodes))
	if r.Reference.NodeCount == 0 {
		if r.Start != 0 || len(r.Nodes) != 0 || !r.Final {
			return errors.New("placer: empty Node Catalog requires one final empty page")
		}
	} else if len(r.Nodes) == 0 || r.Start >= r.Reference.NodeCount || r.Final != (end == r.Reference.NodeCount) {
		return errors.New("placer: Node Catalog final-page marker is invalid")
	}
	return placement.ValidateCatalogPage(r.Nodes)
}

type CatalogSyncResponse struct {
	NextStart uint32 `json:"next_start"`
	Installed bool   `json:"installed"`
}

func (r CatalogSyncResponse) ValidateFor(request CatalogSyncRequest) error {
	if r.NextStart > request.Reference.NodeCount || r.Installed != (r.NextStart == request.Reference.NodeCount) {
		return errors.New("placer: invalid Node Catalog sync response")
	}
	return nil
}

type stagedCatalog struct {
	reference placement.CatalogReference
	pages     map[uint32][]placement.CatalogNode
	bytes     int
	sequence  uint64
	finalSeen bool
}

type catalogCache struct {
	mu        sync.RWMutex
	installed map[string]placement.CatalogSnapshot
	order     []string
	staged    map[string]*stagedCatalog
	sequence  uint64
}

func newCatalogCache() *catalogCache {
	return &catalogCache{
		installed: make(map[string]placement.CatalogSnapshot),
		staged:    make(map[string]*stagedCatalog),
	}
}

func (c *catalogCache) nodes(reference placement.CatalogReference) ([]placement.CatalogNode, error) {
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	snapshot, found := c.installed[reference.Digest]
	c.mu.RUnlock()
	if !found || snapshot.Reference != reference {
		return nil, ErrCatalogUnavailable
	}
	return snapshot.Clone().Nodes, nil
}

func (c *catalogCache) install(request CatalogSyncRequest) (CatalogSyncResponse, error) {
	if err := request.Validate(); err != nil {
		return CatalogSyncResponse{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if snapshot, found := c.installed[request.Reference.Digest]; found {
		if snapshot.Reference != request.Reference {
			return CatalogSyncResponse{}, errors.New("placer: Node Catalog digest identifies another reference")
		}
		return CatalogSyncResponse{NextStart: request.Reference.NodeCount, Installed: true}, nil
	}
	stage := c.staged[request.Reference.Digest]
	if stage == nil {
		c.pruneStagedLocked()
		c.sequence++
		stage = &stagedCatalog{
			reference: request.Reference, pages: make(map[uint32][]placement.CatalogNode), sequence: c.sequence,
		}
		c.staged[request.Reference.Digest] = stage
	} else if stage.reference != request.Reference {
		return CatalogSyncResponse{}, errors.New("placer: Node Catalog digest identifies another staged reference")
	}

	encoded, err := json.Marshal(request.Nodes)
	if err != nil {
		return CatalogSyncResponse{}, err
	}
	if existing, found := stage.pages[request.Start]; found {
		if !reflect.DeepEqual(existing, request.Nodes) {
			return CatalogSyncResponse{}, errors.New("placer: conflicting retry for a Node Catalog page")
		}
	} else {
		end := request.Start + uint32(len(request.Nodes))
		for start, page := range stage.pages {
			pageEnd := start + uint32(len(page))
			if request.Start < pageEnd && start < end {
				return CatalogSyncResponse{}, errors.New("placer: overlapping Node Catalog pages")
			}
		}
		if stage.bytes+len(encoded) > maximumStagedCatalogBytes {
			delete(c.staged, request.Reference.Digest)
			return CatalogSyncResponse{}, errors.New("placer: staged Node Catalog exceeds its memory bound")
		}
		stage.pages[request.Start] = append([]placement.CatalogNode(nil), request.Nodes...)
		stage.bytes += len(encoded)
	}
	stage.finalSeen = stage.finalSeen || request.Final
	next := contiguousCatalogOffset(stage)
	if next != request.Reference.NodeCount || !stage.finalSeen {
		return CatalogSyncResponse{NextStart: next}, nil
	}

	nodes := make([]placement.CatalogNode, 0, request.Reference.NodeCount)
	starts := make([]int, 0, len(stage.pages))
	for start := range stage.pages {
		starts = append(starts, int(start))
	}
	sort.Ints(starts)
	for _, start := range starts {
		nodes = append(nodes, stage.pages[uint32(start)]...)
	}
	if err := placement.ValidateCatalogPage(nodes); err != nil {
		delete(c.staged, request.Reference.Digest)
		return CatalogSyncResponse{}, err
	}
	snapshot, err := placement.NewCatalogSnapshot(
		request.Reference.ClusterID, request.Reference.RegistryGeneration, request.Reference.SystemEpoch,
		request.Reference.RegistryLayoutDigest, request.Reference.Revision, nodes,
	)
	if err != nil || snapshot.Reference != request.Reference {
		delete(c.staged, request.Reference.Digest)
		return CatalogSyncResponse{}, errors.Join(err, errors.New("placer: completed Node Catalog does not match its reference"))
	}
	delete(c.staged, request.Reference.Digest)
	c.installed[request.Reference.Digest] = snapshot
	c.order = append(c.order, request.Reference.Digest)
	for len(c.order) > maximumInstalledCatalogs {
		delete(c.installed, c.order[0])
		c.order = c.order[1:]
	}
	return CatalogSyncResponse{NextStart: request.Reference.NodeCount, Installed: true}, nil
}

func contiguousCatalogOffset(stage *stagedCatalog) uint32 {
	starts := make([]int, 0, len(stage.pages))
	for start := range stage.pages {
		starts = append(starts, int(start))
	}
	sort.Ints(starts)
	next := uint32(0)
	for _, raw := range starts {
		start := uint32(raw)
		if start != next {
			break
		}
		next += uint32(len(stage.pages[start]))
	}
	return next
}

func (c *catalogCache) pruneStagedLocked() {
	if len(c.staged) < maximumStagedCatalogs {
		return
	}
	var oldestKey string
	oldestSequence := ^uint64(0)
	for key, stage := range c.staged {
		if stage.sequence < oldestSequence {
			oldestKey, oldestSequence = key, stage.sequence
		}
	}
	if oldestKey != "" {
		delete(c.staged, oldestKey)
	}
}

func catalogSyncError(reference placement.CatalogReference, err error) error {
	return fmt.Errorf("%w (%s): %v", ErrCatalogUnavailable, reference.Digest, err)
}
