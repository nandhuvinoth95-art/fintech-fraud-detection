# kafka/producer_config.py
# ============================================================
# KAFKA PRODUCER — Streams transactions to Kafka topic
# Simulates real-time payment processing system events
# ============================================================

import json
import time
import random
import uuid
import sys
from datetime import datetime, timedelta
from typing import Optional

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False
    print("⚠️  confluent-kafka not installed. Run: pip install confluent-kafka")

# ============================================================
# KAFKA CONFIGURATION
# ============================================================

LOCAL_KAFKA_CONFIG = {
    "bootstrap.servers": "oyglh-2001-4490-4eb1-7858-9dc6-eba-5445-7a08.run.pinggy-free.link:42623",
    "client.id":         "fraud-detection-producer",
    # Performance settings
    "batch.size":        65536,          # 64KB batch
    "linger.ms":         10,             # Wait 10ms to batch messages
    "compression.type":  "lz4",          # Compress for throughput
    "acks":              "1",            # Leader acknowledgment
    "retries":           3,
    "retry.backoff.ms":  100,
}

CONFLUENT_CLOUD_CONFIG = {
    "bootstrap.servers": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",  # Your endpoint
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "PLAIN",
    "sasl.username":     "YOUR_API_KEY",
    "sasl.password":     "YOUR_API_SECRET",
    "client.id":         "fraud-detection-producer",
    "batch.size":        65536,
    "linger.ms":         10,
    "compression.type":  "lz4",
    "acks":              "all",    # All replicas for cloud durability
}


# ============================================================
# TRANSACTION KAFKA PRODUCER
# ============================================================

class FraudTransactionProducer:
    """
    Streams financial transactions to Kafka topic in real-time.
    Architecture note: In production, this would be the payment
    processing service (like Razorpay/PayU) publishing events.
    Here we simulate that for testing the downstream pipeline.
    """

    def __init__(
        self,
        kafka_config: dict,
        topic: str = "fraud.transactions",
        use_confluent: bool = False
    ):
        if not CONFLUENT_AVAILABLE:
            raise ImportError("Install confluent-kafka: pip install confluent-kafka")

        self.producer  = Producer(kafka_config)
        self.topic     = topic
        self.sent      = 0
        self.failed    = 0

    def _delivery_callback(self, err, msg):
        """Called after each message is acknowledged by Kafka broker."""
        if err:
            self.failed += 1
            print(f"❌ Delivery failed: {err}")
        else:
            self.sent += 1

    def _build_transaction_event(
        self,
        customer_ids: list,
        merchant_ids: list,
        inject_fraud: bool = False
    ) -> dict:
        """Build a single transaction event payload."""
        ts = datetime.now() - timedelta(
             seconds=random.randint(0, 300)  # Slight time skew
        )

        # Fraud injection probability
        is_fraud   = inject_fraud and (random.random() < 0.15)
        amount_mod = random.uniform(8000, 9999) if is_fraud else random.uniform(100, 50000)

        return {
            "transaction_id":        f"TXN_{uuid.uuid4().hex[:16].upper()}",
            "customer_id":           random.choice(customer_ids),
            "merchant_id":           random.choice(merchant_ids),
            "amount":                round(amount_mod, 2),
            "currency":              random.choices(
                                       ["INR","USD","EUR"],
                                       weights=[90, 6, 4]
                                     )[0],
            "timestamp":             ts.isoformat(),
            "transaction_date":      ts.date().isoformat(),
            "location_city":         random.choice([
                                       "Mumbai","Delhi","Bangalore","Chennai","Hyderabad"
                                     ]),
            "location_country":      "IN",
            "latitude":              round(random.uniform(8.0, 37.0), 6),
            "longitude":             round(random.uniform(68.0, 97.0), 6),
            "ip_address":            f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
            "device_id":             f"DEV_{uuid.uuid4().hex[:8].upper()}",
            "device_type":           random.choice(["MOBILE","DESKTOP","POS"]),
            "payment_method":        random.choice(["UPI","CREDIT_CARD","DEBIT_CARD"]),
            "transaction_status":    random.choices(
                                       ["SUCCESS","FAILED","PENDING"],
                                       weights=[87, 9, 4]
                                     )[0],
            "merchant_category":     random.choice([
                                       "GROCERY","ELECTRONICS","TRAVEL",
                                       "CRYPTO_EXCHANGE","JEWELRY"
                                     ]),
            "is_foreign_transaction":random.random() < 0.05,
            "failed_attempt_count":  random.choices([0,1,2,3], weights=[85,8,5,2])[0],
            "response_code":         "00",
            "hour_of_day":           ts.hour,
            "day_of_week":           ts.weekday(),
            "is_weekend":            ts.weekday() >= 5,
            "is_night_transaction":  ts.hour < 5 or ts.hour >= 23,
            # Ground truth (in real system: populated by fraud labeling service)
            "is_fraud":              is_fraud,
            "fraud_type":            random.choice([
                                         "AML_STRUCTURING",
                                         "VELOCITY_FRAUD",
                                         "IMPOSSIBLE_TRAVEL",
                                         "DEVICE_ANOMALY",
                                         "ACCOUNT_TAKEOVER"
                                     ]) if is_fraud else None,
            "fraud_score":           round(random.uniform(0.7, 0.95), 4) if is_fraud
                                     else round(random.uniform(0.0, 0.2), 4),
            # Kafka metadata
            "_kafka_produced_at":    datetime.now().isoformat(),
            "_schema_version":       "1.0",
        }

    def stream_transactions(
        self,
        customer_ids: list,
        merchant_ids: list,
        target_tps: float = 10.0,     # Transactions per second
        duration_seconds: int = 3600, # Run for 1 hour
        inject_fraud: bool = True
    ):
        """
        Stream transactions continuously to Kafka.
        target_tps: Controls message production rate
        duration_seconds: How long to run (None = forever)
        """
        print(f"🚀 Starting Kafka producer: {target_tps} TPS for {duration_seconds}s")
        print(f"   Topic: {self.topic}")
        print(f"   Fraud injection: {inject_fraud}")

        interval   = 1.0 / target_tps
        start_time = time.time()
        batch_log  = 100   # Log every N messages

        while True:
            # Check duration limit
            elapsed = time.time() - start_time
            if duration_seconds and elapsed >= duration_seconds:
                break

            # Build and send event
            event = self._build_transaction_event(
                customer_ids, merchant_ids, inject_fraud
            )
            if event["is_fraud"]:
                print(f"🚨 Sending Fraud: {event['fraud_type']}")
            # Key = customer_id ensures same customer's txns go to same partition
            # This guarantees ordering per customer for velocity detection
            self.producer.produce(
                topic    = self.topic,
                key      = event["customer_id"].encode("utf-8"),
                value    = json.dumps(event).encode("utf-8"),
                callback = self._delivery_callback
            )

            # Periodic flush to ensure delivery
            if self.sent % 500 == 0:
                self.producer.poll(0)

            # Progress logging
            if (self.sent + self.failed) % batch_log == 0 and self.sent > 0:
                print(
                    f"  ✅ Sent: {self.sent:,} | "
                    f"Failed: {self.failed:,} | "
                    f"Elapsed: {elapsed:.0f}s | "
                    f"Rate: {self.sent/elapsed:.1f} TPS"
                )

            # Rate limiting
            time.sleep(interval)

        # Final flush
        self.producer.flush(timeout=30)
        print(f"\n✅ Production complete: {self.sent:,} sent, {self.failed:,} failed")


# ============================================================
# CONSUMER VALIDATION (for testing pipeline connectivity)
# ============================================================

def validate_kafka_consumer(bootstrap_servers: str, topic: str, num_messages: int = 10):
    """
    Validate that Kafka topic is receiving messages correctly.
    Run after starting the producer to verify end-to-end connectivity.
    """
    try:
        from confluent_kafka import Consumer
    except ImportError:
        print("confluent-kafka not installed")
        return

    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id":          "validation-consumer",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(config)
    consumer.subscribe([topic])

    print(f"🔍 Validating {num_messages} messages from {topic}...")
    received = 0

    try:
        while received < num_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                print("  Waiting for messages...")
                continue
            if msg.error():
                print(f"  Error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            print(f"  ✅ [{received+1}] TXN: {event['transaction_id'][:20]} | "
                  f"Amount: {event['amount']:>10,.2f} | "
                  f"Fraud: {event['is_fraud']}")
            received += 1
    finally:
        consumer.close()

    print(f"\n✅ Validation complete: {received} messages received")


# ============================================================
# EXECUTION TRIGGER
# ============================================================

if __name__ == "__main__":
    print("Initializing Fraud Transaction Producer...")

    # 1. Create the producer using your Pinggy configuration
    live_producer = FraudTransactionProducer(
        kafka_config=LOCAL_KAFKA_CONFIG,
        topic="fraud.transactions",
        use_confluent=False
    )

    # 2. Create some dummy IDs for the simulation
    dummy_customers = [f"CUST_LIVE_{i}" for i in range(1, 500)]
    dummy_merchants = [f"MERCH_LIVE_{i}" for i in range(1, 50)]

    # 3. Start the stream! (10 transactions per second)
    try:
        live_producer.stream_transactions(
            customer_ids=dummy_customers,
            merchant_ids=dummy_merchants,
            target_tps=10.0,
            duration_seconds=3600,
            inject_fraud=True
        )
    except KeyboardInterrupt:
        print("\n⏹️ Producer stopped manually.")