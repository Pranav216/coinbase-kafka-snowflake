# coinbase-kafka-snowflake
## DESIGN DECISIONS:

### 1. Separate topics for trades, heartbeats and failures
| Topic | Contents | Purpose |
|---|---|---|
| `coinbase.trades.raw` | `match` and `last_match` messages | Every executed trade; source for the `RAW_TRADES` table |
| `coinbase.heartbeats.raw` | `heartbeat` messages | Carries `last_trade_id` per product, used to detect dropped trades |
| `coinbase.trades.dlq` | Messages the producer could not parse or validate | Dead letter queue: bad data is isolated for inspection and replay instead of being dropped or crashing the pipeline |

Trades and heartbeats have different schemas, and the Snowflake Kafka connector
maps one topic to one table. Separating them keeps each table's schema clean and
means no consumer has to filter by message type.

### 2. `product_id` as the message key
Kafka assigns a partition by hashing the key, so every message for one product
lands on the same partition. Kafka guarantees ordering only within a partition,
so each product's trades are consumed in `trade_id` order. This is required for
two things downstream: detecting gaps in `trade_id`, and getting the correct
open and close price for each OHLCV candle.

### 3. One partition per topic
Partition count is set per topic and is bounded by the number of distinct keys:
with two products (BTC-USD, ETH-USD), extra partitions either sit empty or
receive uneven load (BTC trades far more often than ETH). The feed's volume is
well within what a single partition handles, so one partition per topic is
sufficient. Partitions can be added later but never removed, and adding them
re-maps keys to partitions, so the count starts at the minimum deliberately.

### 4. `side` is the maker's side
Coinbase's `side` field records the resting (maker) order. The aggressor (taker)
is the opposite side, so a match with `side = "sell"` is counted as buy volume.
The pipeline converts this explicitly to `taker_side` before computing
order-flow metrics.



# PRODUCER
Coinbase WS ──► parse ──► route by type ──► validate ──► Kafka
                  │                             │
             bad JSON ──────────────────► invalid ──► coinbase.trades.dlq
