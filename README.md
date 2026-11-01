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


docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
Topic: coinbase.trades.dlq      TopicId: iOlxTpzSSRaAXBq6Dt5FgQ       PartitionCount: 1       ReplicationFactor: 1 Configs: min.insync.replicas=1
        Topic: coinbase.trades.dlq      Partition: 0    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
Topic: coinbase.trades.raw      TopicId: kGFYO9GOT-Cg-G-GBDnYrQ       PartitionCount: 1       ReplicationFactor: 1 Configs: min.insync.replicas=1
        Topic: coinbase.trades.raw      Partition: 0    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
Topic: coinbase.heartbeats.raw  TopicId: YTkV5Dk_TqCQs3e6QZXpIg       PartitionCount: 1       ReplicationFactor: 1 Configs: min.insync.replicas=1
        Topic: coinbase.heartbeats.raw  Partition: 0    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
Topic: __consumer_offsets       TopicId: 3iG6bD0MTmG26b_jtbMxKw       PartitionCount: 50      ReplicationFactor: 1 Configs: compression.type=producer,min.insync.replicas=1,cleanup.policy=compact,segment.bytes=104857600
        Topic: __consumer_offsets       Partition: 0    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 1    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 2    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 3    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 4    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 5    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 6    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 7    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 8    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 9    Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 10   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 11   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 12   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 13   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 14   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 15   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 16   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 17   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 18   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 19   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 20   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 21   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 22   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 23   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 24   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 25   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 26   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 27   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 28   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 29   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 30   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 31   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 32   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 33   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 34   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 35   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 36   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 37   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 38   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 39   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 40   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 41   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 42   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 43   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 44   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 45   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 46   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 47   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 48   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 
        Topic: __consumer_offsets       Partition: 49   Leader: 1     Replicas: 1     Isr: 1  Elr:    LastKnownElr: 


# PRODUCER
Coinbase WS ──► parse ──► route by type ──► validate ──► Kafka
                  │                             │
             bad JSON ──────────────────► invalid ──► coinbase.trades.dlq
