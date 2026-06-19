# eterna-engine

An order matching engine for a prediction market.

- Users submit orders over an HTTP API.
- Orders match by **price-time priority** — integer-tick prices, no floats anywhere.
- Fills are broadcast to connected clients over **WebSocket** in real time.
- The system is correct with **multiple API server instances** running at once.

Tech: Rust (Tokio, Axum), PostgreSQL (SQLx), Redis, WebSockets, Docker, GitHub Actions.

---

## Architecture

The whole design follows from one fact: **matching is inherently a single-writer
problem.** To match by price-time priority you must impose a total order on
incoming orders and apply them one at a time. So the system scales the parts that
*can* be parallel (accepting orders, serving reads, fanning out fills) and keeps
the one part that *can't* — matching — as a single, serialized consumer.

```
 HTTP clients
     │ POST /orders            GET /orderbook            WS /ws
     ▼                              ▼                       ▲
 ┌─────────── API instances ×N (stateless, Axum) ───────────┐
 │  INCR order:seq → XADD orders   read snapshot   broadcast │
 └───┬──────────────────────────────────┬───────────────────┘
     │ XADD                              │ SUBSCRIBE fills
     ▼                                   │
  Redis Stream "orders"  ── ordered intake log ──┐
     │ XREAD (only the leader)                    │
     ▼                                            │
 ┌─────────── matcher (leader-elected, 1 active) ─┤
 │  in-memory OrderBook (price-time) → Fills      │
 │  one txn: fills + maker deltas + offset (PG)   │
 │  PUBLISH fills ───────────────────────────────-┘
 │  write orderbook snapshot (Redis)
     ▼
  Postgres — durable orders, fills, consumed stream offset
```

Three crates do the work, behind a `shared` infrastructure crate:

| Crate | Role |
|-------|------|
| `engine` | Pure price-time matching. No I/O, no async — a deterministic state machine, exhaustively unit-tested. |
| `shared` | Config, wire types, the Redis coordination bus, the Postgres store. |
| `api` | Stateless Axum front-end: `POST /orders`, `GET /orderbook`, `GET /ws`. Run many. |
| `matcher` | The single active consumer that matches and persists. |

---

## The hard parts

### 1. How does the system handle multiple API instances without double-matching?

**Ingestion is decoupled from matching, and matching has exactly one consumer.**

- API instances are **stateless and never match**. On `POST /orders` an instance
  allocates a globally-unique id with a Redis `INCR` and appends the order to a
  single Redis Stream, `orders` (`XADD`). That stream is the **total order** of
  all submissions across every instance.
- A **single matcher** consumes the stream in order (`XREAD`) and applies each
  order to the in-memory book. "Single" is enforced by a **Redis leader lock**
  (`SET key token NX PX <ttl>`, renewed with a compare-and-extend Lua script).
  Several matcher processes may run for availability; exactly one is active, the
  rest stand by. If the leader dies, its lock expires (≤5 s) and a standby takes
  over.

Because only the leader reads the stream and advances the committed offset,
**every order is processed exactly once, in a deterministic order.** No order is
ever matched twice — that's a property of the topology, not of locking individual
orders.

Two layers then make this hold *through crashes and even split-brain*:

1. **Atomic offset.** Each order's full effect (its fills, the maker quantity
   decrements, its own order row) **and** the consumed stream offset commit in a
   *single Postgres transaction*. On restart the matcher rebuilds the book from
   the open orders (in id order, which is arrival order) and resumes `XREAD` from
   the committed offset. So a crash mid-stream re-reads only orders whose effects
   weren't committed — reprocessing is idempotent.
2. **The order id is a primary key — duplicate processing fails closed.** Suppose
   a paused leader resumes after a standby already took over (the classic lock
   hazard). Both might try to process the same stream entry. The second one to
   commit inserts an `orders` row with an id that already exists → primary-key
   violation → the whole transaction aborts and rolls back its fills. Fills are
   published *only after* a successful commit, so the loser never even emits a
   duplicate. Durable state and the live feed both stay correct; the loser simply
   steps down and re-syncs from the DB.

The leader lock makes split-brain *rare*; the single-writer offset and the id
primary key make it *harmless*. A production system would add fencing tokens to
close the window entirely (see §4).

#### Recovery and stale orders on replay

Resume-from-offset (above) is correct for a *brief* crash. A *long* matcher
outage raises a second, subtler problem. The intake stream is a queue of orders
the system already **accepted** — we returned `202 {id}` to each caller. After an
hour-long outage that queue holds an hour of backlog, and blindly replaying it
executes hour-old orders against *today's* book. For a prediction market, where
the event itself may have moved, that fills users on stale intent. This is a
fairness/correctness hazard, not a crash — and because the orders were accepted,
they can't be silently dropped: any expiry must be a **recorded, observable
outcome**, not a gap.

The current code replays the whole backlog unconditionally. Two complementary
guards would fix it; both are deliberately *documented, not built* (see §4 — the
spec defines no time-in-force semantics, and thresholds plus the client-facing
expiry event are product decisions, not ones to guess):

- **Per-order staleness expiry (the right granularity).** The Redis stream id
  already encodes ingest time — it's `<ms>-<seq>`, so the milliseconds are free,
  **with zero schema change**. On replay, an entry older than `MAX_ORDER_AGE_MS`
  is *expired* (recorded `status = 'expired'`, an expiry event emitted, never
  matched or rested) instead of executed; fresh entries process normally. It
  costs nothing in steady state — live orders are milliseconds old — and only
  bites after downtime. Note this is *per-order*, not a blunt "don't replay if
  downtime > X": a global switch would wrongly expire the fresh orders that
  arrived just before recovery too.
- **Global circuit-breaker (catastrophic outage).** If the *oldest* unprocessed
  entry exceeds a much larger limit, the matcher refuses to auto-replay at all —
  it halts and alerts an operator. When something is that wrong, a human decision
  beats auto-executing a massive stale backlog. Per-order expiry handles the
  routine case; the breaker is the "something is very wrong" backstop.

### 2. What data structure did you use for the order book, and why?

Per side, a **`BTreeMap<price, VecDeque<RestingOrder>>`** (`crates/engine/src/book.rs`).

- The **`BTreeMap`** keeps price levels sorted, so the best price is simply the
  first key (asks) or last key (bids) — O(log n) to reach, O(log n) to insert or
  remove a level. Ordered iteration is exactly what a taker needs to *sweep*
  adjacent levels until its limit is reached.
- The **`VecDeque`** within each level preserves arrival order: new orders push to
  the back, matching consumes from the front. That gives **time priority** for
  free, and it makes restart-safe rebuild trivial — replaying open orders in id
  order reproduces the original queues.

Trades execute at the **maker's** price (the resting order sets the price), and
partial fills fall out naturally from `min(taker_qty, maker_qty)` per step.

Alternatives weighed: a flat sorted `Vec` (O(n) inserts), or a `HashMap` of
price→orders plus a separate best-price heap (more bookkeeping, and you still need
ordered iteration to sweep). For a single-symbol book of modest depth, the
`BTreeMap` is the clean fit. The classic HFT optimization — an array indexed by
tick for O(1) best-price — only pays off with a bounded tick range and is noted as
future work, not needed here.

### 3. What breaks first under real production load?

**The matcher.** It is single-threaded *by design* (that's what guarantees order),
and every order funnels through one Redis Stream key and one consumer. The binding
constraint is the **synchronous Postgres transaction per order** — commit/fsync
latency sets the ceiling on matches/sec, and it's one core, one machine. Adding API
instances does nothing for this; they were never the bottleneck.

Secondary pressures, in rough order: the per-batch full-book snapshot write to
Redis is O(book depth); the single `orders` stream key is a write hotspot; and the
Postgres connection/transaction rate. The system **degrades gracefully** — stream
backlog and latency grow, but nothing corrupts — it just won't scale past one
machine's matching throughput.

### 4. What would you build next with another 4 hours?

In priority order:

1. **Shard by market.** Prediction markets are naturally independent — one intake
   stream + one matcher leader *per market*, matchers running in parallel. This is
   the real horizontal-throughput unlock and the data model is one `market_id`
   away from it.
2. **Batch persistence.** One transaction per `XREAD` batch instead of per order,
   plus pipelined fill publishes — directly lifts the per-order commit ceiling
   that §3 identifies.
3. **Sequenced, gap-recoverable WS feed.** Monotonic fill sequence numbers plus a
   snapshot endpoint so a reconnecting client detects and backfills gaps. Today the
   live feed is best-effort and Postgres `fills` is the source of truth.
4. **Stale-order guards on replay** (see §1) — per-order staleness expiry keyed
   off the stream-id timestamp, plus a global circuit-breaker for catastrophic
   outages, so a long matcher downtime never auto-executes a stale backlog.
5. **Fencing tokens** on leadership to close the split-brain window entirely,
   instead of relying on the id-PK backstop.
6. **Cancel / amend**, which needs an id→location index alongside the book.
7. **Operational polish:** Kafka as a partitioned, replayable intake log;
   CloudWatch metrics; property-based tests for the matching invariants.

---

## Data model

The required types (`crates/engine/src/types.rs`), unchanged:

```rust
pub enum Side { Buy, Sell }

pub struct Order { pub id: u64, pub side: Side, pub price: u64, pub qty: u64 }

pub struct Fill {
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub price: u64,
    pub qty: u64,
}
```

---

## HTTP / WebSocket API

| Method | Path | Body / result |
|--------|------|---------------|
| `POST` | `/orders` | `{"side":"buy"\|"sell","price":<u64>,"qty":<u64>}` → `202 {"id":<u64>}`. Matching is async; watch `/ws` for fills. |
| `GET`  | `/orderbook` | `{"bids":[{"price","qty"}…],"asks":[…]}` — bids best-first, asks best-first. |
| `GET`  | `/ws` | WebSocket; server pushes each `Fill` as JSON as matches happen. |
| `GET`  | `/health` | `ok`. |

```sh
curl -XPOST localhost:8080/orders -H 'content-type: application/json' \
     -d '{"side":"sell","price":100,"qty":5}'      # -> {"id":1}
curl localhost:8080/orderbook                        # -> {"bids":[],"asks":[{"price":100,"qty":5}]}
```

---

## Running locally

```sh
docker compose up -d                  # Postgres + Redis

export DATABASE_URL=postgres://eterna:eterna@localhost:5432/eterna
export REDIS_URL=redis://localhost:6379

cargo run --bin matcher &             # the single matcher (migrates on startup)
BIND_ADDR=127.0.0.1:8080 cargo run --bin api &   # instance 1
BIND_ADDR=127.0.0.1:8081 cargo run --bin api &   # instance 2
```

Submit a resting order to one instance and a crossing order to the *other* — they
match (the matcher is the only thing that matches), and the fill arrives on `/ws`
of **both** instances. `GET /orderbook` is identical on both.

Config is env-driven (`crates/shared/src/config.rs`): `DATABASE_URL`, `REDIS_URL`,
`BIND_ADDR` (or `PORT`, which Railway sets), `INSTANCE_ID`.

---

## Testing

```sh
cargo test -p engine                  # matching invariants — no infra needed
cargo test --workspace                # + end-to-end (needs DATABASE_URL & REDIS_URL)
```

- **`engine`** has 11 unit tests covering price priority, FIFO time priority,
  partial fills, sweeping multiple levels, trade-at-maker-price, and quantity
  conservation.
- **`matcher/tests/integration.rs`** runs the *real* matcher binary against live
  Postgres + Redis. It submits 100 orders **concurrently from 100 connections**
  and asserts no order is matched beyond its size (the no-double-match property),
  then kills the matcher with an order resting, submits a crossing order during the
  outage, and asserts a fresh matcher rebuilds the book and clears it. It skips
  cleanly when the env vars are absent.

CI (`.github/workflows/ci.yml`) runs `fmt --check`, `clippy -D warnings`, and the
full test suite with Postgres + Redis service containers.

---

## Deploying on Railway

One Dockerfile builds both binaries; the `SERVICE` env var selects which runs.

1. Add **Postgres** and **Redis** plugins to the project.
2. Create a service from this repo with `SERVICE=api` — Railway injects `PORT`,
   `DATABASE_URL`, `REDIS_URL` (reference the plugin variables). Scale replicas
   freely; they're stateless. Expose it for HTTP/WS.
3. Create a second service from the same repo with `SERVICE=matcher`. No port. Run
   one (or two for failover — the leader lock keeps exactly one active).

---

## Deliberately out of scope

Auth, cancel/amend, multiple markets, rate limiting, exactly-once live WS delivery
(the feed is best-effort; `fills` in Postgres is authoritative), order time-in-force
/ stale-replay guards (replay is currently unconditional — see §1 and §4), Kafka,
and full fencing-token leadership. These are scope cuts for a take-home, called out
here rather than hidden.
