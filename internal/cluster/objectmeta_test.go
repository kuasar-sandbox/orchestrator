package cluster

import "testing"

func TestObjectMetadataRoundTripDoesNotMutateInput(t *testing.T) {
	in := map[string]string{"keep": "value", ObjectMetadataKey: "old"}
	out, err := WithObjectLocation(in, ObjectLocation{Group: "/g"})
	if err != nil {
		t.Fatal(err)
	}
	if in[ObjectMetadataKey] != "old" {
		t.Fatal("WithObjectLocation mutated its input")
	}
	location, err := ObjectLocationFromMetadata(out)
	if err != nil {
		t.Fatal(err)
	}
	if location.Group != "/g" || out["keep"] != "value" {
		t.Fatalf("metadata round trip = %+v, map=%v", location, out)
	}
}

func TestObjectMetadataRejectsMissingIdentity(t *testing.T) {
	if _, err := ObjectLocationFromMetadata(nil); err == nil {
		t.Fatal("missing metadata was accepted")
	}
	if _, err := NodeBuildRefFromMetadata("b", map[string]string{ObjectMetadataKey: `{}`}); err == nil {
		t.Fatal("build metadata without group was accepted")
	}
}
