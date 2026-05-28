# ============================================================
# Kafka Consumer — Read transaction events
# pip install confluent-kafka
# RUN (after producer is running):
#   python consumer.py
#   python consumer.py --from-beginning
# ============================================================

import json
import argparse
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC             = "transactions"
GROUP_ID          = "de-learning-consumer-group"


def main(from_beginning: bool, max_messages: int):
    config = {
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "group.id":           GROUP_ID,
        "auto.offset.reset":  "earliest" if from_beginning else "latest",
        "enable.auto.commit": False,    # manual commit for reliability
        "session.timeout.ms": 10000,
    }

    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    print(f"Consuming from: {TOPIC}")
    print(f"Group ID:       {GROUP_ID}")
    print(f"Starting from:  {'beginning' if from_beginning else 'latest'}")
    print("Press Ctrl+C to stop.\n")

    stats = {"total": 0, "success": 0, "failed": 0, "pending": 0}

    try:
        while stats["total"] < max_messages or max_messages == 0:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                print("  Waiting for messages...")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"  End of partition: {msg.topic()} [{msg.partition()}]")
                else:
                    raise KafkaException(msg.error())
                continue

            # Parse message
            try:
                txn = json.loads(msg.value().decode("utf-8"))
                stats["total"] += 1
                status = txn.get("status", "UNKNOWN")
                stats[status.lower()] = stats.get(status.lower(), 0) + 1

                print(
                    f"  [{stats['total']:4d}] "
                    f"offset={msg.offset():<6} "
                    f"partition={msg.partition()} | "
                    f"{txn['merchant']:12s} | "
                    f"INR {txn['amount_inr']:>8.2f} | "
                    f"{txn['status']:8s} | "
                    f"{txn['city']}"
                )

                # Commit offset after successful processing
                consumer.commit(asynchronous=False)

            except json.JSONDecodeError as e:
                print(f"  [ERROR] Bad JSON: {e}")

    except KeyboardInterrupt:
        print("\nShutting down consumer...")

    finally:
        consumer.close()
        print("\n" + "=" * 40)
        print("CONSUMER SESSION SUMMARY")
        print("=" * 40)
        print(f"  Total messages:  {stats['total']}")
        print(f"  SUCCESS:         {stats.get('success', 0)}")
        print(f"  FAILED:          {stats.get('failed', 0)}")
        print(f"  PENDING:         {stats.get('pending', 0)}")
        if stats["total"] > 0:
            print(f"  Failure rate:    {stats.get('failed',0)/stats['total']*100:.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka transaction consumer")
    parser.add_argument("--from-beginning", action="store_true", help="Read from offset 0")
    parser.add_argument("--max",            type=int, default=0, help="Stop after N messages (0=unlimited)")
    args = parser.parse_args()
    main(args.from_beginning, args.max)
