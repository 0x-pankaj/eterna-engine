//! The matcher: the single point where orders actually match.
//!
//! Correctness under multiple API instances comes from one invariant: at any
//! moment **at most one matcher is active**, and it consumes the intake stream
//! in its total order. Many matcher processes may run for availability, but a
//! Redis leader lock elects exactly one; the rest stand by. Because only the
//! leader reads the stream and advances the committed offset, no order is ever
//! matched twice — by construction, not by locking individual orders.
//!
//! Each processed order is persisted in one transaction (its fills, the maker
//! decrements, the order row, and the consumed stream offset) so a crash
//! recovers exactly: rebuild the book from open orders, resume the stream from
//! the committed offset.

use std::time::{Duration, Instant};

use shared::{db, Bus, Config, Db, OrderBook};

/// How long a leader's claim lives before it auto-expires (ms).
const LEADER_TTL_MS: usize = 5_000;
/// Renew the claim well within the TTL.
const RENEW_AFTER: Duration = Duration::from_millis(1_500);
/// Max time XREAD blocks waiting for new orders (ms). Bounded so we can renew.
const READ_BLOCK_MS: usize = 1_000;
/// Max orders pulled per XREAD.
const READ_COUNT: usize = 128;
/// Backoff when standing by or after an error.
const IDLE_BACKOFF: Duration = Duration::from_millis(1_000);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env();
    let db = Db::connect(&cfg.database_url).await?;
    db.migrate().await?;
    let mut bus = Bus::connect(&cfg.redis_url).await?;
    let token = cfg.instance_id.clone();

    tracing::info!(token = %token, "matcher starting");

    // Forever: win leadership, lead until we lose it, then try again.
    loop {
        while !bus.acquire_leader(&token, LEADER_TTL_MS).await? {
            tracing::debug!("standby: another matcher holds leadership");
            tokio::time::sleep(IDLE_BACKOFF).await;
        }
        tracing::info!(token = %token, "became matcher leader");

        if let Err(e) = lead(&db, &mut bus, &token).await {
            tracing::error!("stepping down after error: {e}");
            tokio::time::sleep(IDLE_BACKOFF).await;
        }
    }
}

/// Run as the active matcher until leadership is lost or an error occurs.
async fn lead(db: &Db, bus: &mut Bus, token: &str) -> anyhow::Result<()> {
    // Recovery: rebuild the book from open orders (arrival order) and resume the
    // stream from the last committed offset.
    let mut book = OrderBook::new();
    for order in db.load_open_orders().await? {
        book.rest(order.id, order.side, order.price, order.qty);
    }
    let mut last_id = db.last_stream_id().await?;
    bus.set_snapshot(&book.snapshot()).await?;
    tracing::info!(%last_id, "recovered book; consuming intake stream");

    let mut last_renew = Instant::now();
    loop {
        // Keep the claim fresh; if we can't, someone else has taken over.
        if last_renew.elapsed() >= RENEW_AFTER {
            if !bus.renew_leader(token, LEADER_TTL_MS).await? {
                tracing::warn!("lost leadership; stepping down");
                return Ok(());
            }
            last_renew = Instant::now();
        }

        let batch = bus.read_orders(&last_id, READ_BLOCK_MS, READ_COUNT).await?;
        let had_work = !batch.is_empty();

        for (stream_id, order) in batch {
            let fills = book.submit(order);
            let traded: u64 = fills.iter().map(|f| f.qty).sum();

            // Persist the order's full effect and the consumed offset atomically.
            // If this fails the book is rebuilt from the DB on the next attempt,
            // and the order is re-read — reprocessing is therefore idempotent.
            let mut tx = db.begin().await?;
            for fill in &fills {
                db::insert_fill(&mut tx, fill).await?;
                db::apply_maker_fill(&mut tx, fill.maker_order_id, fill.qty).await?;
            }
            db::insert_taker_order(&mut tx, &order, order.qty - traded).await?;
            db::set_last_stream_id(&mut tx, &stream_id).await?;
            tx.commit().await?;

            // Only after the fills are durable do we broadcast them.
            for fill in &fills {
                bus.publish_fill(fill).await?;
            }
            last_id = stream_id;
        }

        if had_work {
            bus.set_snapshot(&book.snapshot()).await?;
        }
    }
}
