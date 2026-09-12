package proxyshm

import (
	"encoding/binary"
	"log/slog"
	"net/netip"
	"runtime"
	"sync/atomic"

	connectorvswitch "github.com/kuasar-sandbox/connector/pkg/vswitch"

	"github.com/kuasar-sandbox/orchestrator/internal/routeidentity"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const mmdsSourceSlotCount = int(connectorvswitch.MaxPorts)

// mmapMMDSSourceSlot is a reconstructible MMDS-only reverse-identity hint.
// The primary SID route remains authoritative and contains every credential.
type mmapMMDSSourceSlot struct {
	IPv4      uint32
	Present   uint32
	SandboxID [maxSandboxID]byte
}

type mmdsSourceSlotSnapshot struct {
	index int
	slot  mmapMMDSSourceSlot
}

type mmdsSourceConflict struct {
	index        int
	previousIPv4 uint32
	previousSID  string
	ipv4         uint32
	sid          string
}

func parseMMDSSourceIPv4(raw string) (uint32, bool) {
	return routeidentity.IPv4(raw)
}

func mmdsSourceSlotIndex(ipv4 uint32) int {
	return int(ipv4 % uint32(connectorvswitch.MaxPorts))
}

func mmdsSourceIPv4String(ipv4 uint32) string {
	var bytes [4]byte
	binary.BigEndian.PutUint32(bytes[:], ipv4)
	return netip.AddrFrom4(bytes).String()
}

func activeMMDSSourceRoute(route routesync.RouteEntry) bool {
	return routeidentity.Active(route.State)
}

// lookupMMDSSource performs one fixed-slot lookup, then validates the candidate
// SID against the authoritative primary table before accepting it.
func (t *Table) lookupMMDSSource(ipv4 uint32) (string, bool) {
	if len(t.mmdsSources) != mmdsSourceSlotCount {
		return "", false
	}
	idx := mmdsSourceSlotIndex(ipv4)
	for {
		seq1 := atomic.LoadUint64(&t.header.MMDSSourceSeq)
		if seq1&1 != 0 {
			runtime.Gosched()
			continue
		}

		slot := t.mmdsSources[idx]
		sid := ""
		if slot.Present == statusPresent && slot.IPv4 == ipv4 {
			sid = fixedString(slot.SandboxID[:])
		}
		valid := false
		if sid != "" {
			route, found := t.Lookup(sid)
			if found && routeidentity.Matches(route, ipv4) {
				valid = true
			}
		}

		seq2 := atomic.LoadUint64(&t.header.MMDSSourceSeq)
		if seq1 == seq2 && seq2&1 == 0 {
			if valid {
				return sid, true
			}
			return "", false
		}
	}
}

// snapshotMMDSSourceSlots copies every affected physical slot once. Distinct
// IPv4 values can alias modulo MaxPorts, so deduplication is by slot index.
func (t *Table) snapshotMMDSSourceSlots(ipv4s ...uint32) []mmdsSourceSlotSnapshot {
	snapshots := make([]mmdsSourceSlotSnapshot, 0, len(ipv4s))
	for _, ipv4 := range ipv4s {
		idx := mmdsSourceSlotIndex(ipv4)
		seen := false
		for _, snapshot := range snapshots {
			if snapshot.index == idx {
				seen = true
				break
			}
		}
		if !seen {
			snapshots = append(snapshots, mmdsSourceSlotSnapshot{index: idx, slot: t.mmdsSources[idx]})
		}
	}
	return snapshots
}

func (t *Table) restoreMMDSSourceSlots(snapshots []mmdsSourceSlotSnapshot) {
	if len(snapshots) == 0 || t.readonly {
		return
	}
	startHeaderWrite(&t.header.MMDSSourceSeq)
	for _, snapshot := range snapshots {
		t.mmdsSources[snapshot.index] = snapshot.slot
	}
	finishHeaderWrite(&t.header.MMDSSourceSeq)
}

// publishMMDSSource must be called inside one MMDSSourceSeq write interval.
func (t *Table) publishMMDSSource(ipv4 uint32, sid string) (mmdsSourceConflict, bool) {
	idx := mmdsSourceSlotIndex(ipv4)
	slot := &t.mmdsSources[idx]
	var conflict mmdsSourceConflict
	conflicted := false
	if slot.Present != 0 {
		previousSID := fixedString(slot.SandboxID[:])
		if slot.IPv4 != ipv4 || previousSID != sid {
			conflict = mmdsSourceConflict{
				index: idx, previousIPv4: slot.IPv4, previousSID: previousSID,
				ipv4: ipv4, sid: sid,
			}
			conflicted = true
		}
	}
	slot.Present = statusEmpty
	slot.IPv4 = ipv4
	clearFixed(slot.SandboxID[:])
	copy(slot.SandboxID[:], sid)
	slot.Present = statusPresent
	return conflict, conflicted
}

// removeMMDSSource must be called inside one MMDSSourceSeq write interval. Both
// the exact address and owner SID must match, protecting a reused slot from a
// delayed delete or inactive transition for its previous owner.
func (t *Table) removeMMDSSource(ipv4 uint32, sid string) {
	idx := mmdsSourceSlotIndex(ipv4)
	slot := &t.mmdsSources[idx]
	if slot.Present == 0 || slot.IPv4 != ipv4 || !fixedMMDSSourceSIDEqual(slot.SandboxID[:], sid) {
		return
	}
	*slot = mmapMMDSSourceSlot{}
}

func fixedMMDSSourceSIDEqual(src []byte, sid string) bool {
	if len(sid) > len(src) {
		return false
	}
	for i := range sid {
		if src[i] != sid[i] {
			return false
		}
	}
	return len(sid) == len(src) || src[len(sid)] == 0
}

// clearMMDSSources must be called inside one MMDSSourceSeq write interval.
func (t *Table) clearMMDSSources() {
	for i := range t.mmdsSources {
		t.mmdsSources[i] = mmapMMDSSourceSlot{}
	}
}

func (t *Table) updateMMDSSources(oldRoute routesync.RouteEntry, oldFound bool, incoming routesync.RouteEntry) (mmdsSourceConflict, bool) {
	if t.readonly {
		return mmdsSourceConflict{}, false
	}
	oldIPv4, oldIPv4Valid := parseMMDSSourceIPv4(oldRoute.FloatingIP)
	incomingIPv4, incomingIPv4Valid := parseMMDSSourceIPv4(incoming.FloatingIP)
	startHeaderWrite(&t.header.MMDSSourceSeq)
	if oldFound && activeMMDSSourceRoute(oldRoute) && oldIPv4Valid {
		t.removeMMDSSource(oldIPv4, oldRoute.SandboxID)
	}
	var conflict mmdsSourceConflict
	conflicted := false
	if activeMMDSSourceRoute(incoming) && incomingIPv4Valid {
		conflict, conflicted = t.publishMMDSSource(incomingIPv4, incoming.SandboxID)
	}
	finishHeaderWrite(&t.header.MMDSSourceSeq)
	return conflict, conflicted
}

func (t *Table) removeRouteMMDSSource(route routesync.RouteEntry, found bool) {
	if t.readonly || !found || !activeMMDSSourceRoute(route) {
		return
	}
	ipv4, ok := parseMMDSSourceIPv4(route.FloatingIP)
	if !ok {
		return
	}
	startHeaderWrite(&t.header.MMDSSourceSeq)
	t.removeMMDSSource(ipv4, route.SandboxID)
	finishHeaderWrite(&t.header.MMDSSourceSeq)
}

func (t *Table) clearAllMMDSSources() {
	if t.readonly {
		return
	}
	startHeaderWrite(&t.header.MMDSSourceSeq)
	t.clearMMDSSources()
	finishHeaderWrite(&t.header.MMDSSourceSeq)
}

// rebuildMMDSSources reconstructs the derivative from the authoritative
// primary table. If malformed or duplicate routes exist, deterministic primary
// record order is used and the later valid record wins its direct slot.
func (t *Table) rebuildMMDSSources(log *slog.Logger) {
	if t.readonly {
		return
	}
	type invalidSource struct {
		sid        string
		floatingIP string
	}
	var invalid []invalidSource
	var conflicts []mmdsSourceConflict
	startHeaderWrite(&t.header.MMDSSourceSeq)
	t.clearMMDSSources()
	for i := range t.records {
		var snapshot recordSnapshot
		for {
			var ok bool
			snapshot, ok = readRecordSnapshot(&t.records[i])
			if ok {
				break
			}
			runtime.Gosched()
		}
		if snapshot.status != statusPresent || !activeMMDSSourceRoute(snapshot.entry) {
			continue
		}
		ipv4, valid := parseMMDSSourceIPv4(snapshot.entry.FloatingIP)
		if !valid {
			if snapshot.entry.FloatingIP != "" {
				invalid = append(invalid, invalidSource{sid: snapshot.entry.SandboxID, floatingIP: snapshot.entry.FloatingIP})
			}
			continue
		}
		if conflict, conflicted := t.publishMMDSSource(ipv4, snapshot.entry.SandboxID); conflicted {
			conflicts = append(conflicts, conflict)
		}
	}
	finishHeaderWrite(&t.header.MMDSSourceSeq)
	if log != nil {
		for _, source := range invalid {
			log.Warn("proxyshm: active route has invalid MMDS source IPv4",
				"sid", source.sid, "floating_ip", source.floatingIP)
		}
	}
	for i := range conflicts {
		logMMDSSourceConflict(log, conflicts[i])
	}
}

func logMMDSSourceConflict(log *slog.Logger, conflict mmdsSourceConflict) {
	if log == nil {
		return
	}
	log.Warn("proxyshm: replacing MMDS source slot owner",
		"slot", conflict.index,
		"previous_ip", mmdsSourceIPv4String(conflict.previousIPv4),
		"previous_sid", conflict.previousSID,
		"ip", mmdsSourceIPv4String(conflict.ipv4),
		"sid", conflict.sid,
	)
}
