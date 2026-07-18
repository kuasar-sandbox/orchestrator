package session

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/cespare/xxhash/v2"
)

type Member struct {
	MemberID  string
	Weight    uint32
	Available bool
}

// SelectReconnectTarget uses weighted rendezvous only for a new/reconnecting
// session. Callers never invoke it to migrate an already live Holder session.
func SelectReconnectTarget(nodeID string, members []Member) (string, error) {
	if nodeID == "" {
		return "", errors.New("session: node ID is required")
	}
	selected := ""
	best := math.Inf(1)
	for _, member := range members {
		if member.MemberID == "" || member.Weight == 0 || !member.Available {
			continue
		}
		hash := rendezvousHash(nodeID, member.MemberID)
		uniform := (float64(hash) + 1) / (float64(math.MaxUint64) + 1)
		score := -math.Log(uniform) / float64(member.Weight)
		if score < best || (score == best && member.MemberID < selected) {
			best = score
			selected = member.MemberID
		}
	}
	if selected == "" {
		return "", errors.New("session: no available Registry member")
	}
	return selected, nil
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
