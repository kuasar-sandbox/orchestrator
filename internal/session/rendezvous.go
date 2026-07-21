package session

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
)

type Member struct {
	MemberID  string
	Weight    uint32
	Available bool
}

// RankReconnectTargets returns the weighted-rendezvous winner first and the
// remaining eligible members in the same deterministic order. A reconnecting
// node may try the tail after a target fails; this never migrates a live session.
func RankReconnectTargets(nodeID string, members []Member) ([]string, error) {
	if nodeID == "" {
		return nil, errors.New("session: node ID is required")
	}
	type scoredMember struct {
		memberID string
		score    float64
	}
	scored := make([]scoredMember, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		if member.MemberID == "" || member.Weight == 0 || !member.Available {
			continue
		}
		if _, duplicate := seen[member.MemberID]; duplicate {
			return nil, errors.New("session: duplicate Registry member")
		}
		seen[member.MemberID] = struct{}{}
		hash := rendezvousHash(nodeID, member.MemberID)
		uniform := (float64(hash) + 1) / (float64(math.MaxUint64) + 1)
		score := -math.Log(uniform) / float64(member.Weight)
		scored = append(scored, scoredMember{memberID: member.MemberID, score: score})
	}
	if len(scored) == 0 {
		return nil, errors.New("session: no available Registry member")
	}
	sort.Slice(scored, func(left, right int) bool {
		return scored[left].score < scored[right].score ||
			scored[left].score == scored[right].score && scored[left].memberID < scored[right].memberID
	})
	result := make([]string, len(scored))
	for index := range scored {
		result[index] = scored[index].memberID
	}
	return result, nil
}

func rendezvousHash(nodeID, memberID string) uint64 {
	hash := xxhash.New()
	hash.WriteString("kuasar-session-holder-v1")
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(nodeID)))
	hash.Write(length[:])
	hash.WriteString(nodeID)
	binary.BigEndian.PutUint32(length[:], uint32(len(memberID)))
	hash.Write(length[:])
	hash.WriteString(memberID)
	return hash.Sum64()
}
