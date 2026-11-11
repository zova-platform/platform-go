package outbox

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/zova-platform/platform-go/natsx"
)

const MigrationName = "00001_create_outbox.sql"

//go:embed migrations/00001_create_outbox.sql
var MigrationSQL string

type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func Insert(ctx context.Context, db DBTX, subject string, event natsx.Event) error {
	id := event.GetMeta().GetEventId()
	if id == "" {
		return errors.New("outbox: event meta.event_id is required, use natsx.NewMeta()")
	}
	payload, err := natsx.Marshal(event)
	if err != nil {
		return fmt.Errorf("outbox: marshal %s: %w", subject, err)
	}
	headers, err := json.Marshal(natsx.InjectHeaders(ctx))
	if err != nil {
		return fmt.Errorf("outbox: marshal headers: %w", err)
	}
	if _, err := db.Exec(ctx,
		`INSERT INTO outbox (id, subject, payload, headers) VALUES ($1, $2, $3, $4)`,
		id, subject, payload, headers,
	); err != nil {
		return fmt.Errorf("outbox: insert %s: %w", subject, err)
	}
	return nil
}
