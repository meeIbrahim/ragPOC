package domain

// Source is one retrieved chunk the answer is grounded on. Hash is the document's content hash;
// it stays inside Go and is never sent to the browser.
type Source struct {
	Hash     string
	Filename string
	Snippet  string
	Score    float32
}

// UploadRef links an upload id to its document. Hash is empty while the upload is not hashed
// yet, and DocStatus is empty when no document row exists for the hash.
type UploadRef struct {
	UploadID  string
	Hash      string
	DocStatus DocumentStatus
}

// AnswerPart is one item of an answer stream. The first part carries only Sources (a non-nil,
// possibly empty slice); every later part carries only a Delta of the answer text.
type AnswerPart struct {
	Sources []Source
	Delta   string
}
