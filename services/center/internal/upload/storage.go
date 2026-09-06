package upload

import "time"

type DocumentStorage struct {
	ContentHash string
	ObjectPath  string
	FileName    string
	CreatedAt   time.Time
	Size        int64
}
