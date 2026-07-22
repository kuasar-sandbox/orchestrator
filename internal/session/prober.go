package session

import (
	"context"
	"errors"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/placement"
)

type HolderProbeRPC interface {
	ProbePlacementBatch(context.Context, string, ServeIdentity, []placement.PlacementProbeRequest) ([]placement.PlacementProbeResponse, error)
}

type ProbeResult struct {
	DirectoryEntry DirectoryEntry
	Response       placement.PlacementProbeResponse
}

type DirectoryProber struct {
	directory *Directory
	client    HolderProbeRPC
}

func NewDirectoryProber(directory *Directory, client HolderProbeRPC) (*DirectoryProber, error) {
	if directory == nil || client == nil {
		return nil, errors.New("session: Directory and Holder Probe client are required")
	}
	return &DirectoryProber{directory: directory, client: client}, nil
}

func (p *DirectoryProber) ProbePair(ctx context.Context, identity ServeIdentity, requests []placement.PlacementProbeRequest) []ProbeResult {
	results := make([]ProbeResult, len(requests))
	if len(requests) == 0 {
		return results
	}
	if err := identity.Validate(); err != nil {
		for index, request := range requests {
			results[index].Response = unusableProbe(request, placement.ProbeStale, err.Error())
		}
		return results
	}
	if len(requests) > 2 {
		for index, request := range requests {
			results[index].Response = unusableProbe(request, placement.ProbeReject, "candidate pair exceeds two nodes")
		}
		return results
	}

	type indexedRequest struct {
		index   int
		request placement.PlacementProbeRequest
	}
	groups := make(map[string][]indexedRequest, 2)
	for index, request := range requests {
		entry, found := p.directory.Lookup(request.NodeID)
		if !found {
			results[index].Response = unusableProbe(request, placement.ProbeStale, "node has no current Session Holder")
			continue
		}
		request.ExpectedNodeEpoch = entry.NodeEpoch
		request.ExpectedSessionSeq = entry.SessionSeq
		results[index].DirectoryEntry = entry
		groups[entry.HolderMemberID] = append(groups[entry.HolderMemberID], indexedRequest{index: index, request: request})
	}

	var wait sync.WaitGroup
	for holderID, group := range groups {
		holderID, group := holderID, group
		wait.Add(1)
		go func() {
			defer wait.Done()
			batch := make([]placement.PlacementProbeRequest, len(group))
			for index := range group {
				batch[index] = group[index].request
			}
			responses, err := p.client.ProbePlacementBatch(ctx, holderID, identity, batch)
			if err != nil || len(responses) != len(group) {
				reason := "Holder Probe failed"
				if err != nil {
					reason = err.Error()
				}
				for _, item := range group {
					results[item.index].Response = unusableProbe(item.request, placement.ProbeStale, reason)
				}
				return
			}
			for index, response := range responses {
				item := group[index]
				if response.NodeID != item.request.NodeID || response.NodeEpoch != item.request.ExpectedNodeEpoch ||
					response.SessionSeq != item.request.ExpectedSessionSeq || response.LoadModelVersion != item.request.LoadModelVersion {
					response = unusableProbe(item.request, placement.ProbeStale, "Holder returned a mismatched Probe response")
				}
				results[item.index].Response = response
			}
		}()
	}
	wait.Wait()
	return results
}

func unusableProbe(request placement.PlacementProbeRequest, class placement.ProbeClass, reason string) placement.PlacementProbeResponse {
	return placement.PlacementProbeResponse{
		Class: class, NodeID: request.NodeID,
		NodeEpoch: request.ExpectedNodeEpoch, SessionSeq: request.ExpectedSessionSeq,
		LoadModelVersion: request.LoadModelVersion, Reason: reason,
	}
}
