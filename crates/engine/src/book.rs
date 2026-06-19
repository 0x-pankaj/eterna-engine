use std::collections::{BTreeMap, VecDeque};

use crate::types::{Fill, Level, Order, Side, Snapshot};

/// A resting order on the book. Its price is the map key and its time priority
/// is its position in the level's queue, so we only need id + remaining qty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Resting {
    id: u64,
    qty: u64,
}

/// A price-time-priority limit order book for a single market.
///
/// Each side is a `BTreeMap<price, VecDeque<Resting>>`:
/// - the `BTreeMap` keeps price levels sorted, so the best price is the first
///   (asks) or last (bids) key — O(log n) to reach, O(log n) to insert;
/// - the `VecDeque` per level preserves arrival order, giving time priority:
///   new orders push to the back, matching consumes from the front.
///
/// The book is a pure, synchronous state machine. It performs no I/O and makes
/// no assumptions about threading — the matcher drives it from a single task,
/// which is what makes the whole system correct under concurrent submission.
#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<u64, VecDeque<Resting>>,
    asks: BTreeMap<u64, VecDeque<Resting>>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Match an incoming (taker) order against the book, returning the fills it
    /// generates. Any unfilled remainder rests on the book as a maker.
    ///
    /// A buy crosses the lowest asks while `ask_price <= order.price`; a sell
    /// crosses the highest bids while `bid_price >= order.price`. Trades execute
    /// at the resting maker's price. Partial fills are allowed on both sides.
    pub fn submit(&mut self, order: Order) -> Vec<Fill> {
        match order.side {
            Side::Buy => self.cross(order, Side::Buy),
            Side::Sell => self.cross(order, Side::Sell),
        }
    }

    fn cross(&mut self, mut taker: Order, side: Side) -> Vec<Fill> {
        let mut fills = Vec::new();

        while taker.qty > 0 {
            // Best opposing price level, if it crosses the taker's limit.
            let book_price = match side {
                Side::Buy => self.asks.keys().next().copied(),
                Side::Sell => self.bids.keys().next_back().copied(),
            };
            let price = match book_price {
                Some(p) if crosses(side, taker.price, p) => p,
                _ => break,
            };

            let opposite = match side {
                Side::Buy => &mut self.asks,
                Side::Sell => &mut self.bids,
            };
            let level = opposite.get_mut(&price).expect("price level exists");

            while taker.qty > 0 {
                let maker = match level.front_mut() {
                    Some(m) => m,
                    None => break,
                };
                let traded = taker.qty.min(maker.qty);
                fills.push(Fill {
                    maker_order_id: maker.id,
                    taker_order_id: taker.id,
                    price,
                    qty: traded,
                });
                taker.qty -= traded;
                maker.qty -= traded;
                if maker.qty == 0 {
                    level.pop_front();
                }
            }

            if level.is_empty() {
                opposite.remove(&price);
            }
        }

        if taker.qty > 0 {
            self.rest(taker.id, side, taker.price, taker.qty);
        }
        fills
    }

    /// Place an order on the book without matching. Used both for a taker's
    /// unfilled remainder and to rebuild the book from persisted open orders on
    /// matcher startup. Replaying open orders in ascending id order reproduces
    /// the original arrival order, so time priority survives a restart.
    pub fn rest(&mut self, id: u64, side: Side, price: u64, qty: u64) {
        if qty == 0 {
            return;
        }
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        book.entry(price)
            .or_default()
            .push_back(Resting { id, qty });
    }

    /// Aggregated point-in-time view: bids best (highest) first, asks best
    /// (lowest) first. Used to serve `GET /orderbook`.
    pub fn snapshot(&self) -> Snapshot {
        let bids = self
            .bids
            .iter()
            .rev()
            .map(|(price, level)| Level {
                price: *price,
                qty: level.iter().map(|o| o.qty).sum(),
            })
            .collect();
        let asks = self
            .asks
            .iter()
            .map(|(price, level)| Level {
                price: *price,
                qty: level.iter().map(|o| o.qty).sum(),
            })
            .collect();
        Snapshot { bids, asks }
    }

    /// Best (highest) bid price, if any.
    pub fn best_bid(&self) -> Option<u64> {
        self.bids.keys().next_back().copied()
    }

    /// Best (lowest) ask price, if any.
    pub fn best_ask(&self) -> Option<u64> {
        self.asks.keys().next().copied()
    }
}

/// Does a taker limit at `taker_price` cross a resting level at `book_price`?
fn crosses(side: Side, taker_price: u64, book_price: u64) -> bool {
    match side {
        Side::Buy => book_price <= taker_price,
        Side::Sell => book_price >= taker_price,
    }
}
