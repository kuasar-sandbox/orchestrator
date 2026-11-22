package record

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

const ChunkBytes = 64 << 10
const headerBytes = 10 + 1 + 8 + 4 + 32 + 4

func invalid(s string) error { return fmt.Errorf("%w: %s", storage.ErrInvalid, s) }
func payloadSize(raw []byte) (int, error) {
	if len(raw) < headerBytes || raw[10] != 2 {
		return 0, invalid("record envelope")
	}
	size := binary.BigEndian.Uint64(raw[11:19])
	if size >= storage.MaxTransactionBytes {
		return 0, invalid("payload size")
	}
	return int(size), nil
}
func (t *Tx) encode(phys []byte, key Key, payload []byte, version Version) error {
	raw := make([]byte, headerBytes)
	copy(raw[:10], version[:])
	raw[10] = 2
	binary.BigEndian.PutUint64(raw[11:19], uint64(len(payload)))
	var count uint32
	if len(payload) > ChunkBytes {
		prefix := chunkPrefix(phys)
		for start := 0; start < len(payload); start += ChunkBytes {
			if err := t.write.Put(chunkKey(prefix, count), payload[start:min(start+ChunkBytes, len(payload))]); err != nil {
				return err
			}
			count++
		}
	}
	binary.BigEndian.PutUint32(raw[19:23], count)
	hash := sha256.Sum256(payload)
	copy(raw[23:55], hash[:])
	binary.BigEndian.PutUint32(raw[55:59], uint32(len(key)))
	raw = append(raw, key...)
	if count == 0 {
		raw = append(raw, payload...)
	}
	return t.write.Put(phys, raw)
}
func (t *Tx) decode(phys, raw []byte) (Value, Key, error) {
	size, err := payloadSize(raw)
	if err != nil {
		return Value{}, nil, err
	}
	var version Version
	copy(version[:], raw[:10])
	if !version.IsCommitted() {
		return Value{}, nil, invalid("record version")
	}
	keySize := int(binary.BigEndian.Uint32(raw[55:59]))
	if keySize < 4 || keySize > 9000 || keySize > len(raw)-headerBytes {
		return Value{}, nil, invalid("record logical key")
	}
	key := Key(bytes.Clone(raw[headerBytes : headerBytes+keySize]))
	want, err := physical(key)
	if err != nil || !bytes.Equal(want, phys) {
		return Value{}, nil, invalid("physical key identity")
	}
	count := int(binary.BigEndian.Uint32(raw[19:23]))
	expected := 0
	if size > ChunkBytes {
		expected = (size + ChunkBytes - 1) / ChunkBytes
	}
	if count != expected {
		return Value{}, nil, invalid("chunk count")
	}
	payload := bytes.Clone(raw[headerBytes+keySize:])
	if count > 0 {
		if len(payload) != 0 {
			return Value{}, nil, invalid("chunk envelope suffix")
		}
		prefix := chunkPrefix(phys)
		var after []byte
		payload = make([]byte, 0, size)
		index := 0
		for {
			page, err := t.read.Scan(storage.Range{Begin: prefix, End: storage.PrefixEnd(prefix), After: after}, storage.Limit{Rows: count + 1, Bytes: storage.MaxPageBytes})
			if err != nil {
				return Value{}, nil, err
			}
			for _, row := range page.Rows {
				if index >= count || !bytes.Equal(row.Key, chunkKey(prefix, uint32(index))) || len(row.Value) > ChunkBytes {
					return Value{}, nil, invalid("chunk identity or size")
				}
				payload = append(payload, row.Value...)
				index++
				after = row.Key
			}
			if !page.More {
				break
			}
		}
		if index != count {
			return Value{}, nil, invalid("missing chunks")
		}
	}
	hash := sha256.Sum256(payload)
	if len(payload) != size || !bytes.Equal(hash[:], raw[23:55]) {
		return Value{}, nil, invalid("payload integrity")
	}
	return Value{Exists: true, Version: version, Payload: payload}, key, nil
}
