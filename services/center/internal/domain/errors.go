package domain

import "errors"

// Sentinels shared by the stores and the feature packages. They live here so the sqlite
// store does not have to import upload or hashing to return them.
var (
	ErrIntentNotFound = errors.New("upload intent not found")
	// ErrLostClaim means another worker owns the row now; the caller must not touch it.
	ErrLostClaim = errors.New("lost claim")
)
