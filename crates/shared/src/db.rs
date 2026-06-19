use anyhow::{anyhow, Result};
use engine::{Fill, Order, Side};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};

/// Durable store for orders, fills, and the matcher's stream offset.
///
/// Prices and quantities are `u64` in the engine but stored as `BIGINT` (i64).
/// Prediction-market ticks and sizes sit far below 2^63, so the cast is safe;
/// a production system would use `NUMERIC` or a checked domain type.
#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("../../migrations").run(&self.pool).await?;
        Ok(())
    }

    pub async fn begin(&self) -> Result<Transaction<'_, Postgres>> {
        Ok(self.pool.begin().await?)
    }

    /// Last intake-stream id the matcher durably committed. `"0"` means "from
    /// the beginning" for XREAD.
    pub async fn last_stream_id(&self) -> Result<String> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT last_stream_id FROM matcher_state WHERE id = 1")
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.0).unwrap_or_else(|| "0".to_string()))
    }

    /// All resting orders, in arrival (id) order — replay this into a fresh
    /// `OrderBook` to rebuild exact price-time state after a restart.
    pub async fn load_open_orders(&self) -> Result<Vec<Order>> {
        let rows: Vec<(i64, String, i64, i64)> = sqlx::query_as(
            "SELECT id, side, price, remaining_qty FROM orders \
             WHERE status = 'open' ORDER BY id ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|(id, side, price, qty)| {
                Ok(Order {
                    id: id as u64,
                    side: parse_side(&side)?,
                    price: price as u64,
                    qty: qty as u64,
                })
            })
            .collect()
    }
}

// --- Transaction-scoped writes -------------------------------------------------
//
// The matcher calls these inside one transaction per processed order, together
// with `set_last_stream_id`, so the durable effects and the consumed offset
// commit atomically.

/// Record an incoming order with the quantity left after it matched.
pub async fn insert_taker_order(
    tx: &mut Transaction<'_, Postgres>,
    order: &Order,
    remaining: u64,
) -> Result<()> {
    let status = if remaining > 0 { "open" } else { "filled" };
    sqlx::query(
        "INSERT INTO orders (id, side, price, original_qty, remaining_qty, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(order.id as i64)
    .bind(order.side.as_str())
    .bind(order.price as i64)
    .bind(order.qty as i64)
    .bind(remaining as i64)
    .bind(status)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Decrement a resting maker by the traded quantity, closing it out at zero.
pub async fn apply_maker_fill(
    tx: &mut Transaction<'_, Postgres>,
    maker_id: u64,
    traded: u64,
) -> Result<()> {
    sqlx::query(
        "UPDATE orders \
         SET remaining_qty = remaining_qty - $1, \
             status = CASE WHEN remaining_qty - $1 = 0 THEN 'filled' ELSE status END \
         WHERE id = $2",
    )
    .bind(traded as i64)
    .bind(maker_id as i64)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn insert_fill(tx: &mut Transaction<'_, Postgres>, fill: &Fill) -> Result<()> {
    sqlx::query(
        "INSERT INTO fills (maker_order_id, taker_order_id, price, qty) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(fill.maker_order_id as i64)
    .bind(fill.taker_order_id as i64)
    .bind(fill.price as i64)
    .bind(fill.qty as i64)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn set_last_stream_id(tx: &mut Transaction<'_, Postgres>, stream_id: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO matcher_state (id, last_stream_id) VALUES (1, $1) \
         ON CONFLICT (id) DO UPDATE SET last_stream_id = $1",
    )
    .bind(stream_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn parse_side(s: &str) -> Result<Side> {
    match s {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        other => Err(anyhow!("invalid side in db: {other}")),
    }
}
