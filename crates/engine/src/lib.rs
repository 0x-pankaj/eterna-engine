//! Pure price-time-priority matching engine.
//!
//! This crate has no I/O and no async: it is a deterministic state machine.
//! All of the system's concurrency safety comes from driving a single
//! [`OrderBook`] from one consumer of a totally-ordered intake log — see the
//! `matcher` crate and the README. Keeping the engine pure makes it exhaustively
//! unit-testable, which is where the correctness argument starts.

mod book;
mod types;

pub use book::OrderBook;
pub use types::{Fill, Level, Order, Side, Snapshot};

#[cfg(test)]
mod tests {
    use super::*;

    fn order(id: u64, side: Side, price: u64, qty: u64) -> Order {
        Order {
            id,
            side,
            price,
            qty,
        }
    }

    #[test]
    fn no_match_when_book_empty_rests_order() {
        let mut book = OrderBook::new();
        let fills = book.submit(order(1, Side::Buy, 100, 5));
        assert!(fills.is_empty());
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn exact_cross_fully_fills_both() {
        let mut book = OrderBook::new();
        assert!(book.submit(order(1, Side::Sell, 100, 5)).is_empty());
        let fills = book.submit(order(2, Side::Buy, 100, 5));
        assert_eq!(
            fills,
            vec![Fill {
                maker_order_id: 1,
                taker_order_id: 2,
                price: 100,
                qty: 5,
            }]
        );
        // Book is empty afterwards — nothing left resting on either side.
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn partial_fill_leaves_remainder_resting() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Sell, 100, 3));
        let fills = book.submit(order(2, Side::Buy, 100, 5));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, 3);
        // 2 units of the buy remain resting as the new best bid.
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.best_ask(), None);
        let snap = book.snapshot();
        assert_eq!(snap.bids[0].qty, 2);
    }

    #[test]
    fn taker_sweeps_multiple_levels_up_to_its_limit() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Sell, 100, 2));
        book.submit(order(2, Side::Sell, 101, 2));
        book.submit(order(3, Side::Sell, 102, 2)); // above the taker's limit
        let fills = book.submit(order(4, Side::Buy, 101, 10));
        // Crosses 100 and 101 only; stops at 102. 4 units traded, 6 rest.
        assert_eq!(fills.iter().map(|f| f.qty).sum::<u64>(), 4);
        assert_eq!(fills[0].price, 100);
        assert_eq!(fills[1].price, 101);
        assert_eq!(book.best_bid(), Some(101));
        assert_eq!(book.best_ask(), Some(102));
    }

    #[test]
    fn price_priority_best_price_matches_first() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Sell, 102, 5));
        book.submit(order(2, Side::Sell, 100, 5)); // better ask, arrived later
        let fills = book.submit(order(3, Side::Buy, 102, 5));
        // The 100 ask must fill first despite arriving second.
        assert_eq!(fills[0].maker_order_id, 2);
        assert_eq!(fills[0].price, 100);
    }

    #[test]
    fn time_priority_fifo_within_a_level() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Sell, 100, 5));
        book.submit(order(2, Side::Sell, 100, 5)); // same price, later
        let fills = book.submit(order(3, Side::Buy, 100, 7));
        // Order 1 fully (5), then order 2 partially (2) — strict FIFO.
        assert_eq!(fills[0].maker_order_id, 1);
        assert_eq!(fills[0].qty, 5);
        assert_eq!(fills[1].maker_order_id, 2);
        assert_eq!(fills[1].qty, 2);
    }

    #[test]
    fn trade_executes_at_maker_price_not_taker_price() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Sell, 100, 5)); // maker resting at 100
        let fills = book.submit(order(2, Side::Buy, 105, 5)); // willing to pay 105
                                                              // The buyer gets price improvement: trade at the maker's 100.
        assert_eq!(fills[0].price, 100);
    }

    #[test]
    fn sell_crosses_highest_bid_first() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Buy, 100, 5));
        book.submit(order(2, Side::Buy, 101, 5)); // best bid
        let fills = book.submit(order(3, Side::Sell, 100, 5));
        assert_eq!(fills[0].maker_order_id, 2);
        assert_eq!(fills[0].price, 101);
    }

    #[test]
    fn non_crossing_orders_both_rest_without_trading() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Buy, 99, 5));
        let fills = book.submit(order(2, Side::Sell, 101, 5));
        assert!(fills.is_empty());
        assert_eq!(book.best_bid(), Some(99));
        assert_eq!(book.best_ask(), Some(101));
    }

    #[test]
    fn rest_then_snapshot_aggregates_levels_in_order() {
        // `rest` is the rebuild path used on matcher restart.
        let mut book = OrderBook::new();
        book.rest(1, Side::Buy, 100, 3);
        book.rest(2, Side::Buy, 100, 2); // same level, aggregates to 5
        book.rest(3, Side::Buy, 99, 4);
        book.rest(4, Side::Sell, 101, 7);
        let snap = book.snapshot();
        assert_eq!(
            snap.bids,
            vec![Level { price: 100, qty: 5 }, Level { price: 99, qty: 4 }]
        );
        assert_eq!(snap.asks, vec![Level { price: 101, qty: 7 }]);
    }

    #[test]
    fn quantity_is_conserved_across_a_full_session() {
        // Invariant: every traded unit is removed from exactly one maker and one
        // taker, so total traded volume equals what left the book. We assert the
        // weaker, robust form: traded volume never exceeds supplied volume and
        // the book holds the remainder.
        let mut book = OrderBook::new();
        let mut traded = 0u64;
        traded += book
            .submit(order(1, Side::Sell, 100, 10))
            .iter()
            .map(|f| f.qty)
            .sum::<u64>();
        traded += book
            .submit(order(2, Side::Sell, 101, 10))
            .iter()
            .map(|f| f.qty)
            .sum::<u64>();
        traded += book
            .submit(order(3, Side::Buy, 100, 6))
            .iter()
            .map(|f| f.qty)
            .sum::<u64>();
        traded += book
            .submit(order(4, Side::Buy, 101, 9))
            .iter()
            .map(|f| f.qty)
            .sum::<u64>();

        let snap = book.snapshot();
        let resting: u64 = snap.bids.iter().map(|l| l.qty).sum::<u64>()
            + snap.asks.iter().map(|l| l.qty).sum::<u64>();
        // Supplied 20 sell + 15 buy = 35 units of order flow. Each trade removes
        // equal qty from a buy and a sell, so: sell_in == traded + asks_resting
        // and buy_in == traded + bids_resting.
        let asks_resting: u64 = snap.asks.iter().map(|l| l.qty).sum();
        let bids_resting: u64 = snap.bids.iter().map(|l| l.qty).sum();
        assert_eq!(20, traded + asks_resting);
        assert_eq!(15, traded + bids_resting);
        assert_eq!(resting, asks_resting + bids_resting);
    }
}
