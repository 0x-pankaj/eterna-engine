-- Durable state. The matcher's in-memory book is the source of truth while it
-- runs; these tables let a restarted matcher rebuild the book exactly (open
-- orders, in arrival order) and resume the intake stream from where it left off.

CREATE TABLE IF NOT EXISTS orders (
    id            BIGINT PRIMARY KEY,
    side          TEXT   NOT NULL CHECK (side IN ('buy', 'sell')),
    price         BIGINT NOT NULL,
    original_qty  BIGINT NOT NULL,
    remaining_qty BIGINT NOT NULL,
    status        TEXT   NOT NULL CHECK (status IN ('open', 'filled')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Rebuilding the book only scans resting (open) orders.
CREATE INDEX IF NOT EXISTS orders_open_idx ON orders (id) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS fills (
    seq            BIGSERIAL PRIMARY KEY,
    maker_order_id BIGINT NOT NULL,
    taker_order_id BIGINT NOT NULL,
    price          BIGINT NOT NULL,
    qty            BIGINT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Single-row table holding the last intake-stream id the matcher committed.
-- Updated in the SAME transaction as the fills it produced, so recovery is
-- exactly-once with respect to durable state.
CREATE TABLE IF NOT EXISTS matcher_state (
    id             INT  PRIMARY KEY CHECK (id = 1),
    last_stream_id TEXT NOT NULL
);
