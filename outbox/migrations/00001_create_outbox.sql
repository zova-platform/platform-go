-- +goose Up
CREATE TABLE outbox (
    id uuid PRIMARY KEY,
    subject text NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_unpublished_idx ON outbox (created_at, id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
