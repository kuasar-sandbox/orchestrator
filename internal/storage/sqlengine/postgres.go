package sqlengine

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/storage/observe"
)

func OpenPostgres(ctx context.Context, dsn, namespace string) (*Engine, error) {
	if dsn == "" {
		return nil, errors.New("postgres: explicit connection string required")
	}
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return nil, errors.New("postgres: invalid connection configuration")
	}
	return open(ctx, "postgres:"+dsn, "pgx", dsn, namespace, true)
}
func startListener(dsn, channel string, poller *observe.Poller) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			attempt, stop := context.WithTimeout(ctx, 5*time.Second)
			conn, err := pgx.Connect(attempt, dsn)
			if err == nil {
				_, err = conn.Exec(attempt, "LISTEN "+pgx.Identifier{channel}.Sanitize())
			}
			stop()
			if err == nil {
				// LISTEN has committed before this authoritative reread is requested.
				poller.Wake()
				for ctx.Err() == nil {
					if _, err = conn.WaitForNotification(ctx); err != nil {
						break
					}
					poller.Wake()
				}
			}
			if conn != nil {
				closing, stop := context.WithTimeout(context.Background(), time.Second)
				_ = conn.Close(closing)
				stop()
			}
			if ctx.Err() != nil {
				return
			}
			poller.Invalidate(&storage.Unavailable{Cause: errors.New("postgres notification connection interrupted")})
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return func() { cancel(); <-done }
}
