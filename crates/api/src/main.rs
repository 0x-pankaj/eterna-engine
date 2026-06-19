//! Stateless HTTP front-end. Many instances of this binary run at once; none of
//! them match orders. They allocate ids, append orders to the shared intake
//! stream, and serve the book snapshot — all coordination goes through Redis, so
//! adding instances is just adding capacity.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use shared::{Bus, Config, NewOrder, Order, OrderAck};
use tokio::sync::broadcast;
use tracing_subscriber::EnvFilter;

mod ws;

#[derive(Clone)]
pub(crate) struct AppState {
    bus: Bus,
    /// Local fan-out of fills received from Redis to this instance's WS clients.
    fills: broadcast::Sender<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env();
    let bus = Bus::connect(&cfg.redis_url).await?;

    // One subscriber per instance feeds a broadcast channel shared by all of
    // this instance's WebSocket clients.
    let (fills_tx, _) = broadcast::channel(1024);
    ws::spawn_fanout(bus.clone(), fills_tx.clone());

    let state = AppState {
        bus,
        fills: fills_tx,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/orders", post(post_order))
        .route("/orderbook", get(get_orderbook))
        .route("/ws", get(ws::ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await?;
    tracing::info!(instance = %cfg.instance_id, addr = %cfg.bind_addr, "api listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

/// Accept an order: validate, assign a global id, append it to the intake
/// stream, and return the id. Matching is asynchronous — the caller learns of
/// fills via the WebSocket feed.
async fn post_order(State(state): State<AppState>, Json(new): Json<NewOrder>) -> Response {
    if let Err(msg) = new.validate() {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }

    let mut bus = state.bus.clone();
    let id = match bus.next_order_id().await {
        Ok(id) => id,
        Err(e) => return internal(e),
    };
    let order = Order {
        id,
        side: new.side,
        price: new.price,
        qty: new.qty,
    };
    if let Err(e) = bus.append_order(&order).await {
        return internal(e);
    }

    (StatusCode::ACCEPTED, Json(OrderAck { id })).into_response()
}

/// Current book, read from the matcher-maintained snapshot in Redis. Any
/// instance serves this identically without holding book state.
async fn get_orderbook(State(state): State<AppState>) -> Response {
    let mut bus = state.bus.clone();
    match bus.get_snapshot().await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(e) => internal(e),
    }
}

fn internal<E: std::fmt::Display>(e: E) -> Response {
    tracing::error!("request failed: {e}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}
