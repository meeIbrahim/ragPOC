package sqlite

import "time"

// SetNow replaces the clock of s and of the OutboxStore it embeds, so tests in package
// sqlite_test can pin every timestamp the store writes.
func SetNow(s *Store, now func() time.Time) {
	s.now = now
	s.OutboxStore.now = now
}
