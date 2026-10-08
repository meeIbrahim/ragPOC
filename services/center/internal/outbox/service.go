package outbox

type Event struct {
	Hash          string
	BucketPath    string
	FileName      string
	Size          int64
	Attempt       int
	SchemaVersion int
}
