package hashing

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestSum256KnownVectors(t *testing.T) {
	for in, want := range map[string]string{
		"":    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"abc": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
	} {
		got, err := Sum256(strings.NewReader(in))
		if err != nil || got != want {
			t.Errorf("Sum256(%q) = %s, %v; want %s: the document identity is the SHA-256 of the content (L4)", in, got, err, want)
		}
	}
}

func TestSum256MatchesTheStdlibOnA10MiBInput(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789abcdef"), 10<<20/16) // 10 MiB
	want := sha256.Sum256(content)

	got, err := Sum256(bytes.NewReader(content))
	if err != nil || got != hex.EncodeToString(want[:]) {
		t.Errorf("Sum256(10 MiB) = %s, %v; want %x", got, err, want)
	}
}

func TestSum256ReturnsReadErrors(t *testing.T) {
	boom := errors.New("read failed")
	if _, err := Sum256(io.MultiReader(strings.NewReader("abc"), errReader{boom})); !errors.Is(err, boom) {
		t.Errorf("err = %v; want the reader's error, not a hash of partial content", err)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }
