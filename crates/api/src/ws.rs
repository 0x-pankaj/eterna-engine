//! WebSocket fill feed.
//!
//! Fills are produced by the single matcher and published once to Redis. Each
//! API instance runs one subscriber that fans every fill into a local
//! `broadcast` channel; each connected WebSocket client gets its own receiver.
//! So a client connected to any instance sees every fill, and the matcher stays
//! oblivious to how many clients exist or which instance they're on.

use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use futures::StreamExt;
use shared::Bus;
use tokio::sync::broadcast;

use crate::AppState;

/// Bridge Redis pub/sub into the local broadcast channel, reconnecting on drop.
/// Spawned once per process at startup.
pub fn spawn_fanout(bus: Bus, tx: broadcast::Sender<String>) {
    tokio::spawn(async move {
        loop {
            match bus.fills_pubsub().await {
                Ok(pubsub) => {
                    let mut stream = pubsub.into_on_message();
                    while let Some(msg) = stream.next().await {
                        if let Ok(payload) = msg.get_payload::<String>() {
                            // Err just means no clients are currently connected.
                            let _ = tx.send(payload);
                        }
                    }
                    tracing::warn!("fills subscription ended; reconnecting");
                }
                Err(e) => tracing::error!("fills subscribe failed: {e}"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

pub async fn ws_handler(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    let rx = state.fills.subscribe();
    ws.on_upgrade(move |socket| client_loop(socket, rx))
}

/// Push fills to one client until it disconnects. The feed is push-only; we read
/// the socket only to notice closes and to honour pings.
async fn client_loop(mut socket: WebSocket, mut rx: broadcast::Receiver<String>) {
    loop {
        tokio::select! {
            fill = rx.recv() => match fill {
                Ok(payload) => {
                    if socket.send(Message::Text(payload)).await.is_err() {
                        break;
                    }
                }
                // Slow client fell behind the buffer: skip the gap, keep serving.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("ws client lagged, dropped {n} fills");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                Some(Ok(_)) => {} // ignore anything a client sends
            },
        }
    }
}
