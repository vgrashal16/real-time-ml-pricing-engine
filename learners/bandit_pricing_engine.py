import random
import json
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
# Variables
# -------------------------------
'''
List of available actions (arms) for the bandit.
Each value represents a possible price multiplier the system can choose.
These are discrete choices because bandit algorithms operate over a finite
set of actions, allowing the system to compare their observed rewards over time.

EPSILON controls the exploration–exploitation tradeoff:
- With probability EPSILON, the system explores by choosing a random arm.
- With probability (1 - EPSILON), the system exploits the arm with the
highest average observed reward so far.

A non-zero epsilon ensures the system continues to explore occasionally,
allowing it to adapt to changing user behavior and avoid getting stuck
with a suboptimal pricing strategy.
'''
ARMS = [1.0, 1.2, 1.5, 2.0]
EPSILON = 0.2

# -------------------------------
# Bandit state
# -------------------------------
'''
Bandit state storage.

This nested defaultdict stores learning statistics for each (context, arm) pair.
The structure is:

stats[context][arm] = {
    "count": number of times this arm was chosen in this context,
    "total_reward": cumulative reward observed for this arm in this context
}

defaultdict is used so that:
- New contexts are automatically initialized without manual checks.
- New arms within a context are also initialized on first access.

This allows the learning engine to handle unseen contexts or arms gracefully,
which is essential in streaming and real-world systems where new situations
appear over time.
'''
stats = defaultdict(
    lambda: defaultdict(
        lambda: {
            "count": 0,
            "total_reward": 0.0
        }
    )
)
'''
example of how our stats would look like
{
  ("Airport", "high"): {
      1.0: {"count": 5, "total_reward": 500},
      1.5: {"count": 8, "total_reward": 1160},
      2.0: {"count": 3, "total_reward": 200}
  },
  ("Residential", "low"): {
      1.0: {"count": 10, "total_reward": 900},
      1.2: {"count": 4, "total_reward": 360}
  }
}
'''

# -------------------------------
# Functions
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

'''Selects a pricing arm using an epsilon-greedy strategy.
With probability EPSILON, the function explores by choosing a random arm,
ensuring continued discovery of potentially better actions.

Otherwise, it exploits current knowledge by selecting the arm with the
highest average observed reward for the given context.

IMPORTANT
if s["count"] == 0:
    return arm, "explore"
A forced exploration step is included for arms that have never been tried
in this context, guaranteeing that each arm is evaluated at least once.
'''
def choose_arm(context):

    arm_stats = stats[context]
    # Forced exploration: try all arms at least once per context
    untried_arms = [
        arm for arm in ARMS
        if arm_stats[arm]["count"] == 0
    ]
    if untried_arms:
        return random.choice(untried_arms), "explore"

    #Standard epsilon-greedy exploration

    if random.random() < EPSILON:
        return random.choice(ARMS), "explore"

    '''For specific context, lets say Airport, high
    the arm_stats would look something like
    arm_stats = {
        1.0: {"count": 5, "total_reward": 500},
        1.5: {"count": 8, "total_reward": 1160},
        2.0: {"count": 3, "total_reward": 200}
    }
    '''
    best_arm = None
    best_avg = -1

    for arm in ARMS:
        s = arm_stats[arm]

        if s["count"] == 0:
            return arm, "explore"
        
        avg = s["total_reward"] / s["count"]

        if avg > best_avg:
            best_avg = avg
            best_arm = arm

    return best_arm, "exploit"

def update_stats(context, arm, reward):
    s = stats[context][arm]
    s["count"] += 1
    s["total_reward"] += reward
    print(f"UPDATED {context} arm={arm} → count={s['count']}, total_reward={s['total_reward']}")

# -------------------------------
# Kafka clients
# -------------------------------
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "bandit-engine-v2",
    "auto.offset.reset": "earliest"
})

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVER,
})

# -------------------------------
# Main loop
# -------------------------------

if __name__ == "__main__":
    print("Bandit pricing engine started")

    consumer.subscribe([DEMAND_TOPIC, OUTCOME_TOPIC])

    pending = {}
    '''
    Tracks decisions that have been made but not yet evaluated.
    Because outcomes arrive asynchronously and may be delayed, this dictionary
    temporarily stores the (context, arm) pair for each decision_id.
    When the corresponding outcome arrives, this mapping allows the system
    to correctly assign credit and update the appropriate bandit statistics.
    '''

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            elif msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            topic = msg.topic() #fetching topic to decide whether to send price or consume latest outcome to update the stats accordingly
            event = json.loads(msg.value().decode("utf-8"))

            # Handle demand: choose price
            if topic == DEMAND_TOPIC:
                '''context looks something like ("Airport", "high")'''
                context = (
                    event["location"],
                    demand_ratio(
                        event["active_requests"],
                        event["available_drivers"]
                    )
                )
                print("CONTEXT", context)
                arm, mode = choose_arm(context)

                decision = {
                    "decision_id": event["event_id"],
                    "timestamp": int(datetime.now(timezone.utc).timestamp()),
                    "location": event["location"],
                    "chosen_price": arm,
                    "strategy": f"epsilon_greedy_{mode}",
                    "context": context
                }

                pending[event["event_id"]] = (context, arm) 
                # sample: pending["abc-123"] = (("Airport", "high"), 1.5)

                producer.produce(
                    DECISION_TOPIC,
                    value = json.dumps(decision)
                )
                producer.poll(0)

                print(f"Decision {context} -> {arm} ({mode})")
            
            # Handle outcome: update learning
            elif topic == OUTCOME_TOPIC:
                decision_id = event["decision_id"]
                reward = event["revenue"]

                if decision_id not in pending:
                    continue

                context, arm = pending.pop(decision_id)
                update_stats(context, arm, reward)

                print(f"Stats for bandit learning updated. Updated {context}, arm = {arm}, reward = {reward}")
    
    except KeyboardInterrupt:
        print("Stopping bandit engine")
        
    finally:
        consumer.close()
