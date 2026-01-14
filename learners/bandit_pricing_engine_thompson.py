import random
import json
import os
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer
from collections import defaultdict

# -------------------------------
# Kafka configuration
# -------------------------------
BOOTSTRAP_SERVER = "localhost:9092"

DEMAND_TOPIC = "demand_events"
OUTCOME_TOPIC = "pricing_outcomes"
DECISION_TOPIC = "pricing_decisions"

# -------------------------------
# Bandit configuration
# -------------------------------
ARMS = [1.0, 1.2, 1.5, 2.0]

# -------------------------------
# Thompson Sampling state
# -------------------------------
'''
For each (context, arm), we store:
- successes: number of accepted rides
- failures: number of rejected rides

These define a Beta(successes+1, failures+1) distribution
from which we sample during decision-making.
'''
stats = defaultdict(
    lambda: defaultdict(
        lambda: {
            "successes": 0,
            "failures": 0
        }
    )
)

# -------------------------------
# Persistence helpers
# -------------------------------
STATS_FILE = "bandit_stats_thompson.json"

def context_to_key(context):
    return "|".join(context)

def key_to_context(key):
    loc, bucket = key.split("|")
    return (loc, bucket)

def save_stats():
    data = {}

    for context, arms in stats.items():
        ctx_key = context_to_key(context)
        data[ctx_key] = {}

        for arm, s in arms.items():
            data[ctx_key][str(arm)] = {
                "successes": s["successes"],
                "failures": s["failures"]
            }

    with open(STATS_FILE, "w") as f:
        json.dump(data, f)

    print("Bandit stats saved")

def load_stats():
    if not os.path.exists(STATS_FILE):
        print("No saved stats found, starting fresh")
        return

    with open(STATS_FILE, "r") as f:
        data = json.load(f)

    for ctx_key, arms in data.items():
        context = key_to_context(ctx_key)
        for arm_str, s in arms.items():
            arm = float(arm_str)
            stats[context][arm]["successes"] = s["successes"]
            stats[context][arm]["failures"] = s["failures"]

    print("Bandit stats loaded")

# -------------------------------
# Context helper
# -------------------------------
def demand_ratio(demand, drivers):
    if drivers == 0:
        return "high"
    ratio = demand / drivers
    if ratio < 1.0:
        return "low"
    elif ratio < 2.0:
        return "medium"
    else:
        return "high"

# -------------------------------
# Thompson Sampling decision
# -------------------------------
def choose_arm(context):
    arm_stats = stats[context]

    best_arm = None
    best_sample = -1

    for arm in ARMS:
        s = arm_stats[arm]

        alpha = s["successes"] + 1
        beta = s["failures"] + 1

        sampled_prob = random.betavariate(alpha, beta)

        if sampled_prob > best_sample:
            best_sample = sampled_prob
            best_arm = arm

    return best_arm

# -------------------------------
# Learning update
# -------------------------------
def update_stats(context, arm, accepted):
    s = stats[context][arm]

    if accepted:
        s["successes"] += 1
    else:
        s["failures"] += 1

    print(
        f"UPDATED {context} arm={arm} "
        f"→ successes={s['successes']}, failures={s['failures']}"
    )

# -------------------------------
# Kafka clients
# -------------------------------
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "bandit-pricing-thompson",
    "auto.offset.reset": "latest"
})

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVER,
})

# -------------------------------
# Main loop
# -------------------------------
if __name__ == "__main__":
    load_stats()
    print("Bandit pricing engine (Thompson Sampling) started")

    consumer.subscribe([DEMAND_TOPIC, OUTCOME_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            event = json.loads(msg.value().decode("utf-8"))

            # -----------------------
            # Demand → decision
            # -----------------------
            if topic == DEMAND_TOPIC:
                context = (
                    event["location"],
                    demand_ratio(
                        event["active_requests"],
                        event["available_drivers"]
                    )
                )

                arm = choose_arm(context)

                decision = {
                    "decision_id": event["event_id"],
                    "timestamp": int(datetime.now(timezone.utc).timestamp()),
                    "context": context,
                    "chosen_price": arm,
                    "strategy": "thompson_sampling"
                }

                producer.produce(
                    DECISION_TOPIC,
                    value=json.dumps(decision)
                )
                producer.poll(0)

                print(f"Decision {context} -> {arm} (thompson)")

            # -----------------------
            # Outcome → learning
            # -----------------------
            elif topic == OUTCOME_TOPIC:
                context = tuple(event["context"])
                arm = event["chosen_price"]
                accepted = event["accepted"]

                update_stats(context, arm, accepted)

    except KeyboardInterrupt:
        print("Stopping bandit engine")

    finally:
        save_stats()
        producer.flush()
        consumer.close()
