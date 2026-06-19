use serde::{Deserialize, Serialize};

/// Order side. `Buy` lifts the lowest ask; `Sell` hits the highest bid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
}

/// A submitted order. Prices are integer ticks — there are no floats anywhere
/// in the matching path, so matching is exact and reproducible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub side: Side,
    pub price: u64,
    pub qty: u64,
}

/// A trade. By convention a fill executes at the *maker* (resting) order's
/// price, which is the price-time-priority rule: the order that was on the
/// book first sets the trade price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub price: u64,
    pub qty: u64,
}

/// One aggregated price level in an order-book snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: u64,
    pub qty: u64,
}

/// A point-in-time view of the book, aggregated by price level.
/// `bids` are sorted best (highest) first; `asks` best (lowest) first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}
