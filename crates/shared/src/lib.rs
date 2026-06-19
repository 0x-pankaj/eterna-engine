//! Infrastructure shared by the `api` and `matcher` binaries: configuration,
//! wire types, the Redis coordination bus, and the Postgres store. The
//! interesting design — how independent processes stay correct — lives in
//! [`bus`].

pub mod bus;
pub mod config;
pub mod db;
pub mod model;

pub use bus::Bus;
pub use config::Config;
pub use db::Db;
pub use model::{NewOrder, OrderAck};

// Re-export the engine domain types so the binaries depend on `shared` alone.
pub use engine::{Fill, Level, Order, OrderBook, Side, Snapshot};
