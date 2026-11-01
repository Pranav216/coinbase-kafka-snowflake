import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer

URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCTS = ["BTC-USD", "ETH-USD"]

TOPIC_TRADES = "coinbase.trades.raw"
TOPIC_HEARTBEATS = "coinbase.heartbeats.raw"
TOPIC_DLQ = "coinbase.trades.dlq"

TRADE_FIELDS = {"trade_id", "product_id", "price", "size", "side", "time", "sequence"}
REQUIRED_FIELDS = {
    "match": TRADE_FIELDS,
    "last_match": TRADE_FIELDS,
    "heartbeat": {"product_id", "last_trade_id", "sequence", "time"},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("producer")

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "coinbase-producer",
    "acks": "all",                 # leader waits for all in-sync replicas
    "enable.idempotence": True,    # retries can't create duplicates
    "linger.ms": 50,               # wait up to 50ms to batch messages
    "compression.type": "lz4",
})

stats = {"delivered": 0, "failed": 0, "dlq": 0}


def on_delivery(err, msg):
    """Called by Kafka once per message, after the broker confirms (or rejects) it."""
    if err:
        stats["failed"] += 1
        log.error("Delivery failed to %s: %s", msg.topic(), err)
    else:
        stats["delivered"] += 1


def route(msg: dict) -> str | None:
    # TODO 1: return the topic for this message, based on msg.get("type"):
    #   "match" or "last_match" -> TOPIC_TRADES
    #   "heartbeat"             -> TOPIC_HEARTBEATS
    #   "subscriptions"         -> None   (control message, skip it)
    #   anything else           -> TOPIC_DLQ
    ...


def validate(msg: dict) -> str | None:
    # TODO 2: return None if the message is valid, otherwise a short reason string.
    #   Look up the required fields for msg["type"] in REQUIRED_FIELDS,
    #   find which ones are missing from msg, and if any are missing
    #   return something like "missing fields: ['price']".
    #   Hint: set subtraction -> required - msg.keys()
    ...


def send(topic: str, key: str | None, value: dict):
    producer.produce(
        topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(value).encode("utf-8"),
        on_delivery=on_delivery,
    )
    producer.poll(0)  # serve delivery callbacks without blocking


def handle(raw: str, received_at: str):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        stats["dlq"] += 1
        send(TOPIC_DLQ, None, {"error": f"invalid json: {e}", "raw": raw, "received_at": received_at})
        return

    msg["received_at"] = received_at
    key = msg.get("product_id")
    topic = route(msg)

    if topic is None:
        return

    if topic != TOPIC_DLQ:
        reason = validate(msg)
        if reason:
            topic = TOPIC_DLQ
            msg = {"error": reason, "raw": msg, "received_at": received_at}

    if topic == TOPIC_DLQ:
        stats["dlq"] += 1
        log.warning("Sent to DLQ: %s", msg.get("error", msg.get("type")))

    send(topic, key, msg)


async def stream():
    sub = {"type": "subscribe", "product_ids": PRODUCTS, "channels": ["matches", "heartbeat"]}
    async for ws in websockets.connect(URL, ping_interval=20):   # reconnects with backoff
        try:
            await ws.send(json.dumps(sub))
            log.info("Connected and subscribed to %s", PRODUCTS)
            async for raw in ws:
                handle(raw, datetime.now(timezone.utc).isoformat())
        except websockets.ConnectionClosed as e:
            log.warning("Connection closed (%s). Reconnecting...", e)


async def report():
    while True:
        await asyncio.sleep(10)
        log.info("delivered=%d failed=%d dlq=%d in_flight=%d",
                 stats["delivered"], stats["failed"], stats["dlq"], len(producer))


async def main():
    reporter = asyncio.create_task(report())
    try:
        await stream()
    finally:
        reporter.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        remaining = producer.flush(10)
        log.info("Flushed. %d message(s) undelivered.", remaining)
