# import asyncio, json, websockets
#
# async def main():
#     async with websockets.connect("wss://ws-feed.exchange.coinbase.com") as ws:
#         await ws.send(json.dumps({
#             "type": "subscribe",
#             "product_ids": ["BTC-USD", "ETH-USD"],
#             "channels": ["matches", "heartbeat"]
#         }))
#         while True:
#             print(json.loads(await ws.recv()))
#
# asyncio.run(main())

import asyncio, json, time
import datetime
from datetime import datetime, timezone
import pandas as pd
import websockets
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("explore_feed")


URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCTS = ["BTC-USD", "ETH-USD"]
DURATION_SECS = 600  # 10 minutes

async def capture():
    messages = []
    async with websockets.connect(URL) as ws:
        # send the subscribe message (matches + heartbeat channels)
        # await ws.send(json.dumps({
        #     "type": "subscribe",
        #     "channels": ["matches", "heartbeat"]
        # }))
        log.info("Connected to %s", URL)
        sub = {
            "type": "subscribe",
            "product_ids": PRODUCTS,
            "channels": ["matches", "heartbeat"],
        }
        await ws.send(json.dumps(sub))
        log.info("Subscribed: %s", sub)

        start = time.time()
        last_report = start
        while time.time() - start < DURATION_SECS:
            raw = await ws.recv()
            msg = json.loads(raw)
            # add a 'received_at' field (your local UTC time).
            #   Why: event time vs. ingestion time -> this is how you'll measure latency later.
            received_at = datetime.now(timezone.utc).isoformat()
            msg["received_at"] = received_at
            messages.append(msg)

            if len(messages) <= 5:
                log.info("Message %d: %s", len(messages), msg)

            if time.time() - last_report >= 10:
                elapsed = time.time() - start
                log.info("%d messages in %.0fs (%.1f msg/s)",
                         len(messages), elapsed, len(messages) / elapsed)
                last_report = time.time()

    log.info("Capture finished: %d messages", len(messages))
    return messages

def save(messages):
    # write every message to raw_feed.jsonl (one JSON object per line)
    with open("raw_feed.json", "w") as f:
        for msg in messages:
            f.write(json.dumps(msg) + "\n")
    log.info("Wrote %d lines to raw_feed.jsonl", len(messages))

    df = pd.DataFrame(messages)
    # split df by df["type"] into separate DataFrames (match, heartbeat, last_match)
    df = pd.DataFrame(messages)
    frames = {}
    for msg_type, group in df.groupby("type"):
        if msg_type == "subscriptions":
            continue
        frames[msg_type] = group.dropna(axis=1, how="all")
    log.info("Message types found: %s", df["type"].value_counts().to_dict())
    for msg_type, frame in frames.items():
        log.info("%s: %d rows, columns = %s", msg_type, len(frame), list(frame.columns))

    # build a schema DataFrame: one row per field per message type,
    #   with columns: message_type, field, python_type, example_value
    #   Hint: loop over the first message of each type and use type(value).__name__
    schema_rows = []
    for msg_type, frame in frames.items():
        first = next(m for m in messages if m["type"] == msg_type)
        for field, value in first.items():
            schema_rows.append({
                "message_type": msg_type,
                "field": field,
                "python_type": type(value).__name__,
                "example_value": value,
            })
    schema_df = pd.DataFrame(schema_rows)
    log.info("Schema:\n%s", schema_df.to_string(index=False))

    with pd.ExcelWriter("coinbase_feed_sample.xlsx") as xw:
        # write the schema sheet first, then one sheet per message type
        schema_df.to_excel(xw, sheet_name="schema", index=False)
        for msg_type, frame in frames.items():
            frame.to_excel(xw, sheet_name=msg_type[:31], index=False)
    log.info("Saved coinbase_feed_sample.xlsx with sheets: schema, %s", ", ".join(frames))

if __name__ == "__main__":
    msgs = asyncio.run(capture())
    save(msgs)
    print(f"Captured {len(msgs)} messages")