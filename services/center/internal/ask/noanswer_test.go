package ask

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// The Python file sits outside this module, so the test runs on the host checkout only. A
// missing file or line fails rather than skips: a silent skip would hide the drift.
func TestNoRelevantAnswerMatchesPython(t *testing.T) {
	path := filepath.Join("..", "..", "..", "rag-service", "src", "rag", "query", "service.py")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v (the drift check needs the Python source)", path, err)
	}
	m := regexp.MustCompile(`(?m)^NO_RELEVANT_ANSWER = "([^"\\]*)"$`).FindSubmatch(src)
	if m == nil {
		t.Fatalf(`%s has no line NO_RELEVANT_ANSWER = "...": update this test if the constant moved`, path)
	}
	if got := string(m[1]); got != NoRelevantAnswer {
		t.Fatalf("Python NO_RELEVANT_ANSWER = %q, Go NoRelevantAnswer = %q: a short-circuited ask must read the same as Python's", got, NoRelevantAnswer)
	}
}
