"""
Replays data/raw/events.jsonl into the betstream.events Kafka topic.
Keys each message by player_id so one player's events stay ordered on one partition.
"""
import json
import sys
from pathlib import Path

from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "betstream.events"
EVENTS_FILE = Path("data/raw/events.jsonl")


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}", file=sys.stderr)


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    sent = 0
    with open(EVENTS_FILE) as f:
        for line in f:
            event = json.loads(line)
            key = event["player_id"]
            producer.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=line.strip().encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            sent += 1
            if sent % 1000 == 0:
                print(f"sent {sent}")

    producer.flush()
    print(f"done. sent {sent} events to {TOPIC}")


if __name__ == "__main__":
    main()