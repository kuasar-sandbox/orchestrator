package metadata

import (
	"encoding/base64"
	"errors"
)

// String is a bounded opaque token for the node-list stream bookmark.
func (t DirectoryToken) String() string {
	b := make([]byte, DirectoryBuckets*10)
	for i := range t {
		copy(b[i*10:], t[i][:])
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
func ParseDirectoryToken(text string) (DirectoryToken, error) {
	var token DirectoryToken
	if text == "" {
		return token, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil || len(b) != DirectoryBuckets*10 {
		return token, errors.New("metadata: invalid directory token")
	}
	for i := range token {
		copy(token[i][:], b[i*10:(i+1)*10])
		if !token[i].IsZero() && !token[i].IsCommitted() {
			return DirectoryToken{}, errors.New("metadata: incomplete directory token")
		}
	}
	return token, nil
}
