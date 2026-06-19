//! End-to-end tests that run the real `matcher` binary against live Postgres
//! and Redis. They are gated on `DATABASE_URL` + `REDIS_URL` (set by CI and by
//! `docker compose up`) and skip cleanly when those aren't present, so a plain
//! `cargo test` without infra still passes.
//!
//! Both scenarios live in one `#[test]` run sequentially: they share the Redis
//! stream and the Postgres tables, so they must not run concurrently.

use std::process::{Child, Command};
use std::time::{Duration, Instant};

use shared::{Bus, Order, Side};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Kills the spawned matcher when it drops, so a panicking assert never leaks a
/// background process.
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_matcher(db_url: &str, redis_url: &str, id: &str) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_matcher"))
        .env("DATABASE_URL", db_url)
        .env("REDIS_URL", redis_url)
        .env("INSTANCE_ID", id)
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn matcher");
    ChildGuard(child)
}

/// Submit an order the same way the API does: allocate a global id, append to
/// the intake stream.
async fn submit(redis_url: &str, side: Side, price: u64, qty: u64) {
    let mut bus = Bus::connect(redis_url).await.unwrap();
    let id = bus.next_order_id().await.unwrap();
    bus.append_order(&Order {
        id,
        side,
        price,
        qty,
    })
    .await
    .unwrap();
}

async fn scalar(pool: &PgPool, sql: &str) -> i64 {
    let (v,): (Option<i64>,) = sqlx::query_as(sql).fetch_one(pool).await.unwrap();
    v.unwrap_or(0)
}

/// Wipe Redis and the tables for a clean slate between scenarios.
async fn reset(pool: &PgPool, redis_url: &str) {
    sqlx::migrate!("../../migrations").run(pool).await.unwrap();
    sqlx::query("TRUNCATE orders, fills, matcher_state")
        .execute(pool)
        .await
        .unwrap();
    let client = redis::Client::open(redis_url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
}

/// Poll `cond(pool)` until true or timeout, returning whether it became true.
async fn wait_for(pool: &PgPool, timeout: Duration, sql: &str, target: i64) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if scalar(pool, sql).await == target {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test]
async fn end_to_end_matcher() {
    let (Ok(db_url), Ok(redis_url)) = (std::env::var("DATABASE_URL"), std::env::var("REDIS_URL"))
    else {
        eprintln!("skipping: set DATABASE_URL and REDIS_URL to run integration tests");
        return;
    };
    let pool = PgPoolOptions::new().connect(&db_url).await.unwrap();

    concurrent_orders_match_exactly_once(&pool, &db_url, &redis_url).await;
    matcher_recovers_after_restart(&pool, &db_url, &redis_url).await;
}

/// The core safety property: 100 orders submitted *concurrently* from 100
/// independent connections (standing in for many API instances) must match
/// without any order being matched beyond its quantity. A balanced book at one
/// price clears completely regardless of arrival interleaving.
async fn concurrent_orders_match_exactly_once(pool: &PgPool, db_url: &str, redis_url: &str) {
    reset(pool, redis_url).await;
    let _matcher = spawn_matcher(db_url, redis_url, "it-conservation");

    const N: usize = 100; // 50 sells + 50 buys, qty 2 each @ price 100
    let mut tasks = Vec::new();
    for i in 0..N {
        let ru = redis_url.to_string();
        tasks.push(tokio::spawn(async move {
            let side = if i % 2 == 0 { Side::Sell } else { Side::Buy };
            submit(&ru, side, 100, 2).await;
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // All 100 orders recorded and nothing left resting.
    assert!(
        wait_for(
            pool,
            Duration::from_secs(20),
            "SELECT count(*) FROM orders",
            N as i64
        )
        .await,
        "matcher did not process all orders in time"
    );
    assert!(
        wait_for(
            pool,
            Duration::from_secs(20),
            "SELECT count(*) FROM orders WHERE remaining_qty <> 0",
            0,
        )
        .await,
        "book did not fully clear"
    );

    // 100 sell units cross 100 buy units => exactly 100 units traded.
    assert_eq!(
        scalar(pool, "SELECT sum(qty)::bigint FROM fills").await,
        100
    );
    // No order was matched beyond its size.
    assert_eq!(
        scalar(pool, "SELECT count(*) FROM orders WHERE remaining_qty < 0").await,
        0,
        "an order was over-matched (double match)"
    );
    // Per-order conservation: executed qty == fills attributed to it as taker
    // plus as maker. Any double-counting would break this for some order.
    assert_eq!(
        scalar(
            pool,
            "SELECT count(*) FROM orders o WHERE (o.original_qty - o.remaining_qty) <> \
             COALESCE((SELECT sum(qty) FROM fills WHERE taker_order_id = o.id), 0) + \
             COALESCE((SELECT sum(qty) FROM fills WHERE maker_order_id = o.id), 0)"
        )
        .await,
        0,
        "fills do not reconcile with order execution"
    );
}

/// A resting order survives a matcher crash: the book is rebuilt from Postgres
/// and the stream resumes, so an order that arrives *while the matcher is down*
/// still matches against it on restart.
async fn matcher_recovers_after_restart(pool: &PgPool, db_url: &str, redis_url: &str) {
    reset(pool, redis_url).await;

    // Phase 1: a sell rests on the book.
    let matcher = spawn_matcher(db_url, redis_url, "it-recovery-1");
    submit(redis_url, Side::Sell, 100, 5).await;
    assert!(
        wait_for(
            pool,
            Duration::from_secs(20),
            "SELECT count(*) FROM orders WHERE status = 'open' AND remaining_qty = 5",
            1,
        )
        .await,
        "resting sell was not persisted"
    );

    // Kill the matcher. A crossing buy arrives during the outage.
    drop(matcher);
    submit(redis_url, Side::Buy, 100, 5).await;

    // Phase 2: a fresh matcher rebuilds the book and clears the cross.
    let _matcher = spawn_matcher(db_url, redis_url, "it-recovery-2");
    assert!(
        wait_for(
            pool,
            Duration::from_secs(20),
            "SELECT sum(qty)::bigint FROM fills",
            5
        )
        .await,
        "recovered matcher did not match the order placed during downtime"
    );
    assert!(
        wait_for(
            pool,
            Duration::from_secs(20),
            "SELECT count(*) FROM orders WHERE remaining_qty <> 0",
            0,
        )
        .await,
        "book did not clear after recovery"
    );
}
