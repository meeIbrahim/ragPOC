package outbox

import (
	"context"
	"database/sql"
	"log"
	"time"

	"ragingest/internal/db"
	"ragingest/internal/stream"
)

func Start(ctx context.Context, conn *sql.DB, publisher stream.Publisher, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweep(ctx, conn, publisher)
			}
		}
	}()
}

func sweep(ctx context.Context, conn *sql.DB, publisher stream.Publisher) {
	docs, err := db.UnpublishedHashes(conn)
	if err != nil {
		log.Printf("outbox: sweep query failed: %v", err)
		return
	}
	for _, d := range docs {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := publisher.Publish(pctx, d.HashID, d.ObjectPath, "application/pdf")
		cancel()
		if err != nil {
			log.Printf("outbox: publish retry failed for %s: %v", d.HashID, err)
			continue
		}
		if err := db.MarkPublished(conn, d.HashID); err != nil {
			log.Printf("outbox: mark published failed for %s: %v", d.HashID, err)
		}
	}
}
