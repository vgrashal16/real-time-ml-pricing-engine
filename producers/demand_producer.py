import json
import time
import uuid
import random
from datetime import datetime, timezone

from confluent_kafka import Producer


# -------------------------------
# Kafka configuration
# -------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "demand_events"


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")


# -------------------------------
# Simulation configuration
# -------------------------------
LOCATIONS = ["Airport", "Downtown", "IT_Park", "Residential"]
WEATHER = ["Clear", "Rain", "Cloudy"]


def generate_demand_event():
    active_requests = random.randint(5, 50)
    available_drivers = random.randint(5, 40)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
        "location": random.choice(LOCATIONS),
        "active_requests": active_requests,
        "available_drivers": available_drivers,
        "weather": random.choice(WEATHER)
    }


# -------------------------------
# Main loop
# -------------------------------
if __name__ == "__main__":
    print("🚕 Demand producer started. Sending events to Kafka...")

    while True:
        event = generate_demand_event()

        producer.produce(
            TOPIC_NAME,
            value=json.dumps(event),
            callback=delivery_report
        )

        producer.poll(0)
        time.sleep(random.uniform(1, 2))
