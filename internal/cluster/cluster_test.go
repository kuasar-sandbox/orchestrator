package cluster

import (
	"reflect"
	"testing"
)

func TestMemberViewOwnersStableAndCapped(t *testing.T) {
	v1 := MemberView{Version: 1, Members: []string{"m3", "m1", "m2"}}
	v2 := MemberView{Version: 1, Members: []string{"m1", "m2", "m3"}}
	a, err := v1.Owners("/g", 9)
	if err != nil {
		t.Fatal(err)
	}
	b, err := v2.Owners("/g", 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("owners depend on input order: %v vs %v", a, b)
	}
	if len(a) != 3 {
		t.Fatalf("owners len=%d, want capped to 3", len(a))
	}
}

func TestProjectNodeListPreservesSplitEndpoints(t *testing.T) {
	entry := ProjectNodeList(NodeRecord{
		NodeID:       "n1",
		APIEndpoint:  "10.0.0.1:7443",
		DataEndpoint: "10.0.0.1:8443",
	})
	if entry.APIEndpoint != "10.0.0.1:7443" || entry.DataEndpoint != "10.0.0.1:8443" {
		t.Fatalf("projected endpoints: api=%q data=%q", entry.APIEndpoint, entry.DataEndpoint)
	}
}
