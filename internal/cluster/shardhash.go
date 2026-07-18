package cluster

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
)

const (
	ShardHashV1Version = "ShardHashV1"

	DefaultVirtualShardCount uint32 = 4096
	DefaultRouteBucketCount  uint32 = 16
	DefaultBuildBucketCount  uint32 = 16
	DefaultReplicationFactor uint32 = 3
	DefaultPlacementN        uint32 = 4
)

const shardHashV1Magic = "kuasar-shard-hash-v1"

type shardHashDomain byte

const (
	routeBucketDomain shardHashDomain = 0x01
	routeShardDomain  shardHashDomain = 0x02
	buildBucketDomain shardHashDomain = 0x03
	buildShardDomain  shardHashDomain = 0x04
)

var errInvalidShardHashUTF8 = errors.New("cluster: shard hash field is not valid UTF-8")

func RouteBucketHash(group, routeKey string) (uint64, error) {
	return shardHashStrings(routeBucketDomain, group, routeKey)
}

func RouteShardHash(group string, bucket uint32) (uint64, error) {
	return shardHashStringUint32(routeShardDomain, group, bucket)
}

func BuildBucketHash(group, buildID string) (uint64, error) {
	return shardHashStrings(buildBucketDomain, group, buildID)
}

func BuildShardHash(group string, bucket uint32) (uint64, error) {
	return shardHashStringUint32(buildShardDomain, group, bucket)
}

func RouteShardFor(group, routeKey string, bucketCount, shardCount uint32) (bucket, shard uint32, err error) {
	if bucketCount == 0 || shardCount == 0 {
		return 0, 0, errors.New("cluster: shard and bucket counts must be non-zero")
	}
	bh, err := RouteBucketHash(group, routeKey)
	if err != nil {
		return 0, 0, err
	}
	bucket = uint32(bh % uint64(bucketCount))
	sh, err := RouteShardHash(group, bucket)
	if err != nil {
		return 0, 0, err
	}
	return bucket, uint32(sh % uint64(shardCount)), nil
}

func BuildShardFor(group, buildID string, bucketCount, shardCount uint32) (bucket, shard uint32, err error) {
	if bucketCount == 0 || shardCount == 0 {
		return 0, 0, errors.New("cluster: shard and bucket counts must be non-zero")
	}
	bh, err := BuildBucketHash(group, buildID)
	if err != nil {
		return 0, 0, err
	}
	bucket = uint32(bh % uint64(bucketCount))
	sh, err := BuildShardHash(group, bucket)
	if err != nil {
		return 0, 0, err
	}
	return bucket, uint32(sh % uint64(shardCount)), nil
}

func shardHashStrings(domain shardHashDomain, fields ...string) (uint64, error) {
	h := xxhash.New()
	_, _ = h.WriteString(shardHashV1Magic)
	_, _ = h.Write([]byte{byte(domain)})
	for _, field := range fields {
		if !utf8.ValidString(field) {
			return 0, errInvalidShardHashUTF8
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(field)))
		_, _ = h.Write(length[:])
		_, _ = h.WriteString(field)
	}
	return h.Sum64(), nil
}

func shardHashStringUint32(domain shardHashDomain, field string, value uint32) (uint64, error) {
	if !utf8.ValidString(field) {
		return 0, errInvalidShardHashUTF8
	}
	h := xxhash.New()
	_, _ = h.WriteString(shardHashV1Magic)
	_, _ = h.Write([]byte{byte(domain)})
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(len(field)))
	_, _ = h.Write(encoded[:])
	_, _ = h.WriteString(field)
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = h.Write(encoded[:])
	return h.Sum64(), nil
}
