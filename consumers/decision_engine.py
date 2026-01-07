import json
from confluent_kafka import Consumer, Producer
from datetime import datetime, timezone


# -------------------------------
# Kafka configuration
# -------------------------------
BOOTSTRAP_SERVERS = "localhost:9092"

DEMAND_TOPIC = "demand_events"
DECISION_TOPIC = "pricing_decisions"


consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "decision-engine-group",
    "auto.offset.reset": "latest"
})

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS
})


# -------------------------------
# Pricing logic (rule-based)
# -------------------------------
def decide_price(demand, drivers):
    if drivers == 0:
        return 2.0

    ratio = demand / drivers

    if ratio < 1.0:
        return 1.0
    elif ratio < 2.0:
        return 1.2
    elif ratio < 3.0:
        return 1.5
    else:
        return 2.0


# -------------------------------
# Main loop
# -------------------------------
if __name__ == "__main__":
    print("💰 Decision engine started...")

    consumer.subscribe([DEMAND_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))

            demand = event["active_requests"]
            drivers = event["available_drivers"]

            price = decide_price(demand, drivers)

            decision_event = {
                "decision_id": event["event_id"],
                "timestamp": int(datetime.now(timezone.utc).timestamp()),
                "location": event["location"],
                "active_requests": demand,
                "available_drivers": drivers,
                "chosen_price": price,
                "strategy": "rule_based"
            }

            producer.produce(
                DECISION_TOPIC,
                value=json.dumps(decision_event)
            )
            producer.poll(0)

            print(f"📈 Decision → {decision_event}")

    except KeyboardInterrupt:
        print("🛑 Stopping decision engine...")

    finally:
        consumer.close()
