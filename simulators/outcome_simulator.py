import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer


# -------------------------------
# Kafka configuration
# -------------------------------
BOOTSTRAP_SERVER = "localhost:9092"

DECISION_TOPIC = "pricing_decisions"
OUTCOME_TOPIC = "pricing_outcomes"


# -------------------------------
# Kafka clients
# -------------------------------
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "outcome-simulator-group",
    "auto.offset.reset": "latest"
})

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVER
})


# -------------------------------
# Acceptance logic (simulated world)
# -------------------------------

def acceptance_probabily(location, price):
    base_prob = {
        "Airport": 0.9,
        "Downtown": 0.75,
        "IT_Park": 0.7,
        "Residential": 0.6
    }.get(location, 0.65)

    penalty = (price - 1) * 0.25

    acceptance_probabily = base_prob - penalty
    return max(0.05, acceptance_probabily)

''' This function simulates how likely a user is to accept a ride at a given price.
We model acceptance probability using two simple, intuitive assumptions:
1) Different locations have different baseline willingness to pay.
For example, Airport users are more urgent and price-tolerant than
Residential users, so they start with a higher base acceptance probability.
.get(location, 0.65) -> This is the fallback acceptance probability.

2) As the price multiplier increases above the base price (1.0x),
the probability of acceptance decreases linearly.
This represents users becoming less willing to accept higher surge prices.

The final acceptance probability is computed as:
base_probability(location) - price_penalty

A small lower bound (5%) is enforced to avoid zero or negative probabilities,
reflecting the real-world idea that a small fraction of users will accept
almost any price. This logic is intentionally simple and transparent, as it
serves as a controllable simulation of user behavior rather than a learned model.'''

# -------------------------------
# Main loop
# -------------------------------
if __name__ == "__main__":
    print("Outcome simulator started...")

    consumer.subscribe([DECISION_TOPIC])
    try:
        while True: 
            msg = consumer.poll(1.0)
            '''This loop keeps the consumer running continuously, making it a long-lived service.
            consumer.poll(timeout) asks Kafka for a new message.
            - If a message arrives within `timeout` seconds, it is returned.
            - If no message arrives, `None` is returned.
            The timeout prevents the loop from blocking forever and allows the program
            to remain responsive (e.g., for shutdowns or other checks).'''
            
            if msg is None:
                continue

            elif msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            '''
            Kafka messages are received as raw bytes, not Python objects.
            msg.value() returns the message payload as a byte string (e.g., b'{"key": "value"}').
            decode("utf-8") converts the byte string into a regular Python string.
            json.loads() then parses that JSON-formatted string and converts it into
            a Python dictionary so the data can be accessed using normal key lookups.
            decision = json.loads(msg.value().decode("utf-8"))'''


            decision = json.loads(msg.value().decode("utf-8"))

            location = decision["location"]
            price = decision["chosen_price"]

            prob = acceptance_probabily(location, price)
            '''random.random() generates a floating-point number between 0.0 and 1.0.
            This value is compared against the computed acceptance probability.

            If the random value is less than the probability, the ride is considered accepted.
            Over many events, this produces accept/reject behavior that statistically matches
            the desired acceptance probability, simulating real user decision-making.
            accepted = random.random() < prob'''

            accepted = random.random() < prob

            revenue = 0
            if accepted:
                    revenue = int(100 * price)
            
            outcome_event = {
                "decision_id": decision["decision_id"],
                "timestamp": int(datetime.now(timezone.utc).timestamp()),
                "context": decision["context"],
                "chosen_price": decision["chosen_price"],
                "accepted": accepted,
                "revenue": revenue
            }


            # Simulate delay (real systems have lag)
            time.sleep(random.uniform(2, 5))

            producer.produce(
                OUTCOME_TOPIC,
                value = json.dumps(outcome_event)
            )

            '''Kafka producers send messages asynchronously.
            Calling producer.poll() allows the producer to process background tasks such as:
            - sending queued messages to the broker
            - handling delivery confirmations
            - retrying failed sends

            A timeout of 0 means "do not block"; it simply processes any pending events.
            This call is necessary to ensure messages are actually delivered.
            producer.poll(0)'''
            
            producer.poll(0)
                        
            print(f"Outcome → {outcome_event}")
    
    except KeyboardInterrupt:
        print("Stopping outcome simulator...")
    
    finally:
        consumer.close()
