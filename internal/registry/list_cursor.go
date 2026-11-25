package registry

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"golang.org/x/crypto/chacha20poly1305"
)

const publicListCursorPrefix = "cl1_"
const maxPublicListCursorBytes = 16 << 10

type publicListCursorCodec struct {
	mu   sync.Mutex
	aead cipher.AEAD
}

func (r *Registry) publicListCursorCipher(ctx context.Context) (cipher.AEAD, error) {
	r.listCursor.mu.Lock()
	defer r.listCursor.mu.Unlock()
	if r.listCursor.aead != nil {
		return r.listCursor.aead, nil
	}
	key, err := r.stores.listCursors.GetOrCreatePublicListCursorKey(ctx)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	// XChaCha20's 192-bit nonce supports independent random nonces across
	// Registry peers sharing a long-lived key, without a global nonce counter.
	// Cache only after durable key initialization and cipher creation succeed.
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return nil, err
	}
	r.listCursor.aead = aead
	return aead, nil
}

func publicListCursorAAD(group string, state SandboxState, apiKey string) []byte {
	keyHash := sha256.Sum256([]byte(apiKey))
	// JSON length/delimiter encoding keeps all client-controlled fields
	// unambiguous; the domain binds both the operation and token version.
	aad, _ := json.Marshal(struct {
		Domain  string
		Group   string
		State   SandboxState
		KeyHash [32]byte
	}{"cluster-public-sandbox-list/v1", group, state, keyHash})
	return aad
}

func (r *Registry) encodePublicListCursor(ctx context.Context, group string, state SandboxState, apiKey string, cursor metadata.Cursor) (string, error) {
	if cursor == "" {
		return "", nil
	}
	// Encrypt the internal cursor bytes without a second base64 expansion.
	plain, err := base64.RawURLEncoding.DecodeString(string(cursor))
	if err != nil {
		return "", metadata.ErrInvalidCursor
	}
	aead, err := r.publicListCursorCipher(ctx)
	if err != nil {
		return "", err
	}
	var nonce [chacha20poly1305.NonceSizeX]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	sealed := aead.Seal(nonce[:], nonce[:], plain, publicListCursorAAD(group, state, apiKey))
	return publicListCursorPrefix + base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (r *Registry) decodePublicListCursor(ctx context.Context, group string, state SandboxState, apiKey, token string) (metadata.Cursor, error) {
	if token == "" {
		return "", nil
	}
	encoded, ok := strings.CutPrefix(token, publicListCursorPrefix)
	if !ok || len(token) > maxPublicListCursorBytes {
		return "", metadata.ErrInvalidCursor
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) < chacha20poly1305.NonceSizeX+chacha20poly1305.Overhead+1 {
		return "", metadata.ErrInvalidCursor
	}
	aead, err := r.publicListCursorCipher(ctx)
	if err != nil {
		return "", err
	}
	plain, err := aead.Open(nil, raw[:chacha20poly1305.NonceSizeX], raw[chacha20poly1305.NonceSizeX:], publicListCursorAAD(group, state, apiKey))
	if err != nil || len(plain) == 0 {
		return "", metadata.ErrInvalidCursor
	}
	return metadata.Cursor(base64.RawURLEncoding.EncodeToString(plain)), nil
}
