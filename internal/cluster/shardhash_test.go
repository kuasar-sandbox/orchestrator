package cluster

import "testing"

func TestShardHashV1CanonicalVectors(t *testing.T) {
	routeBucketHash, err := RouteBucketHash("/acme/dev", "sandbox-alpha")
	if err != nil {
		t.Fatal(err)
	}
	if routeBucketHash != 0xfb613cf28ba0620a {
		t.Fatalf("route bucket hash = %#016x", routeBucketHash)
	}
	routeBucket, routeShard, err := RouteShardFor("/acme/dev", "sandbox-alpha", 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if routeBucket != 10 || routeShard != 2453 {
		t.Fatalf("route = bucket %d shard %d, want 10/2453", routeBucket, routeShard)
	}
	routeShardHash, err := RouteShardHash("/acme/dev", 10)
	if err != nil {
		t.Fatal(err)
	}
	if routeShardHash != 0xa3de294066fe0995 {
		t.Fatalf("route shard hash = %#016x", routeShardHash)
	}

	buildBucketHash, err := BuildBucketHash("/acme/dev", "build-01HXYZ")
	if err != nil {
		t.Fatal(err)
	}
	if buildBucketHash != 0x099636275f2619cb {
		t.Fatalf("build bucket hash = %#016x", buildBucketHash)
	}
	buildBucket, buildShard, err := BuildShardFor("/acme/dev", "build-01HXYZ", 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if buildBucket != 11 || buildShard != 1329 {
		t.Fatalf("build = bucket %d shard %d, want 11/1329", buildBucket, buildShard)
	}
	buildShardHash, err := BuildShardHash("/acme/dev", 11)
	if err != nil {
		t.Fatal(err)
	}
	if buildShardHash != 0xf643ddcd4afb8531 {
		t.Fatalf("build shard hash = %#016x", buildShardHash)
	}
}

func TestShardHashV1RejectsInvalidInputs(t *testing.T) {
	if _, err := RouteBucketHash("/group", string([]byte{0xff})); err == nil {
		t.Fatal("invalid UTF-8 route key accepted")
	}
	if _, _, err := BuildShardFor("/group", "build", 0, 4096); err == nil {
		t.Fatal("zero bucket count accepted")
	}
}
