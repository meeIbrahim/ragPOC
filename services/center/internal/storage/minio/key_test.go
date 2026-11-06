package minio

import "testing"

func TestIngestionObjectKeyShardsByTheHashPrefix(t *testing.T) {
	const h = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got, want := IngestionObjectKey(h), "ba/78/"+h; got != want {
		t.Errorf("IngestionObjectKey = %q; want %q (aa/bb/<hash>, PRD L5)", got, want)
	}
}

// Callers pass only a Go-computed 64-hex SHA-256 (L4), so the key does not validate its input.
// This pins that contract: a short hash panics rather than producing a malformed key.
func TestIngestionObjectKeyPanicsOnAHashShorterThanItsShards(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("IngestionObjectKey(\"abc\") did not panic; if it now validates, update this test and its callers")
		}
	}()
	IngestionObjectKey("abc")
}
