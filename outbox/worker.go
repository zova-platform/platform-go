package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultPollInterval  = 200 * time.Millisecond
	defaultBatchSize     = 100
	defaultRetention     = 24 * time.Hour
	defaultPurgeInterval = time.Minute
)

type Publisher interface {
	PublishRaw(ctx context.Context, subject, msgID string, payload []byte, headers map[string]string) error
}

type DB interface {
	DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Config struct {
	PollInterval  time.Duration
	BatchSize     int
	Retention     time.Duration
	PurgeInterval time.Duration
}

type Worker struct {
	db  DB
	pub Publisher
	cfg Config
}

func NewWorker(db DB, pub Publisher, cfg Config) *Worker {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.Retention <= 0 {
		cfg.Retention = defaultRetention
	}
	if cfg.PurgeInterval <= 0 {
		cfg.PurgeInterval = defaultPurgeInterval
	}
	return &Worker{db: db, pub: pub, cfg: cfg}
}

func (w *Worker) Run(ctx context.Context) error {
	poll := time.NewTicker(w.cfg.PollInterval)
	defer poll.Stop()
	purge := time.NewTicker(w.cfg.PurgeInterval)
	defer purge.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-poll.C:
			w.drain(ctx)
		case <-purge.C:
			if _, err := w.Purge(ctx); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "outbox: purge failed", "error", err)
			}
		}
	}
}

func (w *Worker) drain(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := w.PublishPending(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.ErrorContext(ctx, "outbox: publish failed", "error", err)
			}
			return
		}
		if n < w.cfg.BatchSize {
			return
		}
	}
}

func (w *Worker) PublishPending(ctx context.Context) (int, error) {
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := tx.Query(ctx,
		`SELECT id::text, subject, payload, headers
		   FROM outbox
		  WHERE published_at IS NULL
		  ORDER BY created_at, id
		  LIMIT $1
		    FOR UPDATE SKIP LOCKED`, w.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox: select pending: %w", err)
	}
	type pending struct {
		id, subject string
		payload     []byte
		headers     map[string]string
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.subject, &p.payload, &p.headers); err != nil {
			rows.Close()
			return 0, fmt.Errorf("outbox: scan: %w", err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("outbox: read pending: %w", err)
	}

	var published []string
	var publishErr error
	for _, p := range batch {
		if err := w.pub.PublishRaw(ctx, p.subject, p.id, p.payload, p.headers); err != nil {
			publishErr = fmt.Errorf("outbox: publish %s: %w", p.id, err)
			break
		}
		published = append(published, p.id)
	}

	if len(published) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])`, published); err != nil {
			return 0, fmt.Errorf("outbox: mark published: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("outbox: commit: %w", err)
	}
	return len(published), publishErr
}

func (w *Worker) Purge(ctx context.Context) (int64, error) {
	tag, err := w.db.Exec(ctx,
		`DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < $1`,
		time.Now().Add(-w.cfg.Retention))
	if err != nil {
		return 0, fmt.Errorf("outbox: purge: %w", err)
	}
	return tag.RowsAffected(), nil
}
