# eterna-engine

An order matching engine for a prediction market.

- Users submit orders over an HTTP API.
- Orders match by **price-time priority** (integer-tick prices, no floats).
- Fills are broadcast to connected clients over **WebSocket**.
- The system runs correctly with **multiple API server instances** at once.

## Tech stack

Rust (Tokio, Axum), PostgreSQL (SQLx), Redis, WebSockets, Docker, GitHub Actions.

## Status

Work in progress — see commit history. The design and the answers to the hard
questions (multi-instance correctness, order-book data structure, first failure
under load, what's next) are written up at the bottom of this file as they land.

## Local development

```sh
docker compose up -d        # postgres + redis
cargo test --workspace      # run the test suite
```
