package upload

import "time"

type DocumentIngestion struct {
	ContentHash string
	ObjectPath  string
	FileName    string
	CreatedAt   time.Time
	Size        int64
}
