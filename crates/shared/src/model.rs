use engine::Side;
use serde::{Deserialize, Serialize};

/// Request body for `POST /orders`. The server assigns the id, so callers don't
/// send one.
#[derive(Debug, Clone, Deserialize)]
pub struct NewOrder {
    pub side: Side,
    pub price: u64,
    pub qty: u64,
}

impl NewOrder {
    /// Reject degenerate orders before they reach the intake log. Zero price or
    /// zero qty can never trade and would just pollute the book.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.price == 0 {
            return Err("price must be greater than zero");
        }
        if self.qty == 0 {
            return Err("qty must be greater than zero");
        }
        Ok(())
    }
}

/// Response body for `POST /orders`: the assigned order id. Matching happens
/// asynchronously; fills arrive on the WebSocket feed.
#[derive(Debug, Clone, Serialize)]
pub struct OrderAck {
    pub id: u64,
}
