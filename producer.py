# ============================================================
# Kafka Producer — Synthetic Transaction Events
# Simulates a real-time payment stream
#
# SETUP:
#   pip install confluent-kafka faker
#   Start Kafka first: docker-compose up -d
#   Create topic:
#     docker exec kafka kafka-topics --create \
#       --topic transactions --bootstrap-server localhost:9092 \
#       --partitions 3 --replication-factor 1
#
# RUN:
#   python producer.py
#   python producer.py --count 100 --delay 0.1
# ============================================================

import json
import time
import random
import argparse
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer, KafkaException

fake = Faker("en_IN")   # Indian locale for realistic names

# ── Config ─────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC             = "transactions"

MERCHANTS = [
    "Swiggy", "Zomato", "Amazon", "Flipkart", "BigBasket",
    "Myntra", "Nykaa", "PhonePe", "Paytm", "BookMyShow",
    "Uber", "Ola", "MakeMyTrip", "IRCTC", "Zepto",
]
CATEGORIES = {
    "Swiggy": "food_delivery",     "Zomato": "food_delivery",
    "Amazon": "ecommerce",         "Flipkart": "ecommerce",
    "BigBasket": "grocery",        "Myntra": "fashion",
    "Nykaa": "beauty",             "PhonePe": "finance",
    "Paytm": "finance",            "BookMyShow": "entertainment",
    "Uber": "transport",           "Ola": "transport",
    "MakeMyTrip": "travel",        "IRCTC": "travel",
    "Zepto": "grocery",
}
CITIES     = ["Chennai", "Mumbai", "Bangalore", "Delhi", "Hyderabad", "Pune"]
CARD_TYPES = ["VISA", "Mastercard", "RuPay", "Amex"]
STATUSES   = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"]


def generate_transaction() -> dict:
    merchant  = random.choice(MERCHANTS)
    amount    = round(random.uniform(50, 5000), 2)
    # Occasional high-value transactions (fraud simulation)
    if random.random() < 0.02:
        amount = round(random.uniform(10000, 50000), 2)

    return {
        "transaction_id":  fake.uuid4(),
        "timestamp":       datetime.utcnow().isoformat() + "Z",
        "customer_id":     f"CUST{random.randint(1000, 9999)}",
        "customer_name":   fake.name(),
        "merchant":        merchant,
        "category":        CATEGORIES[merchant],
        "amount_inr":      amount,
        "city":            random.choice(CITIES),
        "card_type":       random.choice(CARD_TYPES),
        "card_last4":      str(random.randint(1000, 9999)),
        "status":          random.choice(STATUSES),
        "is_international": random.random() < 0.05,
        "device":          random.choice(["mobile_app", "web", "pos_terminal"]),
    }


def delivery_callback(err, msg):
    if err:
        print(f"  [ERROR] Delivery failed: {err}")
    else:
        print(
            f"  [OK] Topic={msg.topic()} "
            f"Partition={msg.partition()} "
            f"Offset={msg.offset()}"
        )


def main(count: int, delay: float, verbose: bool):
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id":         "de-learning-producer",
        "acks":              "all",        # wait for all replicas to ack
        "retries":           3,
        "retry.backoff.ms":  500,
    }

    try:
        producer = Producer(config)
        print(f"Connected to Kafka: {BOOTSTRAP_SERVERS}")
        print(f"Publishing to topic: {TOPIC}")
        print(f"Sending {count if count > 0 else 'unlimited'} messages...\n")

        sent = 0
        while count == 0 or sent < count:
            txn  = generate_transaction()
            key  = txn["customer_id"]
            body = json.dumps(txn, ensure_ascii=False)

            producer.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=body.encode("utf-8"),
                callback=delivery_callback if verbose else None,
            )
            producer.poll(0)    # trigger callbacks without blocking

            sent += 1
            if sent % 10 == 0 or verbose:
                print(f"  Sent #{sent}: {txn['merchant']:12s} | "
                      f"INR {txn['amount_inr']:>8.2f} | "
                      f"{txn['status']:8s} | {txn['city']}")

            time.sleep(delay)

        producer.flush(timeout=10)
        print(f"\nDone — {sent} messages sent to '{TOPIC}'")

    except KafkaException as e:
        print(f"Kafka error: {e}")
        print("Is Kafka running? Try: docker-compose up -d")
    except KeyboardInterrupt:
        print(f"\nStopped. Sent {sent} messages.")
        producer.flush(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic transaction Kafka producer")
    parser.add_argument("--count",   type=int,   default=50,  help="Number of messages (0=infinite)")
    parser.add_argument("--delay",   type=float, default=0.5, help="Delay between messages in seconds")
    parser.add_argument("--verbose", action="store_true",     help="Print delivery callbacks")
    args = parser.parse_args()
    main(args.count, args.delay, args.verbose)
