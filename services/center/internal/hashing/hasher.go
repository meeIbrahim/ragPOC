// Package hashing provides content hashing helpers.
package hashing

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// Sum256 returns the hex-encoded SHA-256 digest of r's contents.
func Sum256(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
