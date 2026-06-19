use anyhow::{Context, Result};
use engine::{Fill, Order, Snapshot};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client};

const ORDER_SEQ: &str = "order:seq";
const ORDERS_STREAM: &str = "orders";
const FILLS_CHANNEL: &str = "fills";
const SNAPSHOT_KEY: &str = "orderbook:snapshot";
const LEADER_KEY: &str = "matcher:leader";

/// Redis-backed coordination layer shared by every process. This is where the
/// multi-instance design lives:
///
/// - `next_order_id` (INCR) hands out globally-unique ids across all API nodes.
/// - `append_order` (XADD) writes to a single ordered stream — the total order
///   that makes matching deterministic regardless of which API node ingested.
/// - `read_orders` (XREAD) is consumed only by the current matcher leader.
/// - `acquire_leader` / `renew_leader` elect exactly one active matcher.
/// - `publish_fill` / `fills_pubsub` fan fills out to every API node's clients.
/// - `set_snapshot` / `get_snapshot` mirror the book so any node serves reads.
#[derive(Clone)]
pub struct Bus {
    client: Client,
    conn: redis::aio::MultiplexedConnection,
}

impl Bus {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = Client::open(url).context("invalid REDIS_URL")?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .context("connect to redis")?;
        Ok(Self { client, conn })
    }

    /// Allocate the next global order id. Atomic across all API instances.
    pub async fn next_order_id(&mut self) -> Result<u64> {
        let id: u64 = self.conn.incr(ORDER_SEQ, 1).await?;
        Ok(id)
    }

    /// Append an order to the ordered intake stream. Returns its stream id.
    pub async fn append_order(&mut self, order: &Order) -> Result<String> {
        let payload = serde_json::to_string(order)?;
        let id: String = self
            .conn
            .xadd(ORDERS_STREAM, "*", &[("data", payload.as_str())])
            .await?;
        Ok(id)
    }

    /// Read orders strictly after `last_id`, blocking up to `block_ms` for new
    /// entries. Only the matcher leader calls this.
    pub async fn read_orders(
        &mut self,
        last_id: &str,
        block_ms: usize,
        count: usize,
    ) -> Result<Vec<(String, Order)>> {
        let opts = StreamReadOptions::default().block(block_ms).count(count);
        let reply: StreamReadReply = self
            .conn
            .xread_options(&[ORDERS_STREAM], &[last_id], &opts)
            .await?;

        let mut out = Vec::new();
        for key in reply.keys {
            for entry in key.ids {
                if let Some(data) = entry.get::<String>("data") {
                    out.push((entry.id, serde_json::from_str::<Order>(&data)?));
                }
            }
        }
        Ok(out)
    }

    /// Broadcast a fill to all subscribers (every API node's WebSocket fan-out).
    pub async fn publish_fill(&mut self, fill: &Fill) -> Result<()> {
        let payload = serde_json::to_string(fill)?;
        let _: i64 = self.conn.publish(FILLS_CHANNEL, payload).await?;
        Ok(())
    }

    /// A subscriber stream of fill messages. The caller drives it with
    /// `into_on_message()`. Kept here so the channel name has one definition.
    pub async fn fills_pubsub(&self) -> Result<redis::aio::PubSub> {
        let mut pubsub = self.client.get_async_pubsub().await?;
        pubsub.subscribe(FILLS_CHANNEL).await?;
        Ok(pubsub)
    }

    /// Mirror the current book so any API node can serve `GET /orderbook`
    /// without holding book state itself.
    pub async fn set_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let payload = serde_json::to_string(snapshot)?;
        let _: () = self.conn.set(SNAPSHOT_KEY, payload).await?;
        Ok(())
    }

    pub async fn get_snapshot(&mut self) -> Result<Snapshot> {
        let payload: Option<String> = self.conn.get(SNAPSHOT_KEY).await?;
        match payload {
            Some(p) => Ok(serde_json::from_str(&p)?),
            None => Ok(Snapshot::default()),
        }
    }

    /// Try to become the matcher leader. `SET key token NX PX ttl` succeeds for
    /// exactly one caller; the lock auto-expires if that process dies.
    pub async fn acquire_leader(&mut self, token: &str, ttl_ms: usize) -> Result<bool> {
        let res: Option<String> = redis::cmd("SET")
            .arg(LEADER_KEY)
            .arg(token)
            .arg("NX")
            .arg("PX")
            .arg(ttl_ms)
            .query_async(&mut self.conn)
            .await?;
        Ok(res.is_some())
    }

    /// Extend our leadership, but only if we still hold it (compare-and-extend,
    /// so we never stomp a lock a successor already took over).
    pub async fn renew_leader(&mut self, token: &str, ttl_ms: usize) -> Result<bool> {
        let script = redis::Script::new(
            r"if redis.call('get', KEYS[1]) == ARGV[1] then
                  return redis.call('pexpire', KEYS[1], ARGV[2])
              else
                  return 0
              end",
        );
        let ok: i64 = script
            .key(LEADER_KEY)
            .arg(token)
            .arg(ttl_ms)
            .invoke_async(&mut self.conn)
            .await?;
        Ok(ok == 1)
    }
}
