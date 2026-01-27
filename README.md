# Real-Time Dynamic Pricing with Kafka & Thompson Sampling
## Overview

This project implements a real-time dynamic pricing engine that learns optimal pricing strategies online from streaming data. It uses Apache Kafka for event-driven communication and Bayesian Multi-Armed Bandits (Beta–Bernoulli Thompson Sampling) to continuously adapt prices based on observed user behavior.

Unlike traditional batch-trained ML models, this system learns while it runs, making pricing decisions per event and updating its beliefs as outcomes arrive asynchronously.

## Why This Project Exists

Dynamic pricing is fundamentally a sequential decision-making problem:

* Decisions must be made in real time

* Feedback arrives later and asynchronously

* The system’s actions influence future data

* User behavior is stochastic and non-stationary

This makes static ML models, offline training, and simple regression approaches ill-suited. Instead, this project models pricing as a contextual bandit problem, which is a production-proven approach used in pricing, advertising, and recommendation systems.

## Core Concepts
### Contextual Bandits

Pricing decisions are conditioned on context, which in this project consists of:

* Location (e.g. Airport, Downtown)

* Demand intensity (low / medium / high, derived from demand–supply ratio)

Each (context, price) pair is treated as an independent bandit arm.

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.12+
- pip / virtualenv
- Git

## Start Kafka

From the project root:

```bash
docker compose up -d
```
This starts a single-node Kafka broker in KRaft mode (no ZooKeeper).

Verify Kafka is running before proceeding.

## Set up Python environment

```bash
python -m venv venv
source venv/bin/activate   # Linux / macOS
venv\Scripts\activate      # Windows

pip install -r requirements.txt
```

## Start the Demand Producer

```bash
python producers/demand_producer.py
```
Produces events to the demand_events Kafka topic.

## Start the Bandit Pricing Engine

```bash
python learners/bandit_pricing_engine.py
```

This component:

* Consumes demand events

* Chooses prices using Thompson Sampling

* Emits pricing decisions

* Learns from outcomes in real time

* Persists learning state to disk

## Start the Outcome Simulator

```bash
python simulators/outcome_simulator.py
```
Consumes pricing decisions and produces outcomes to pricing_outcomes.

### Observing Learning

As the system runs, you should observe:

* Pricing decisions being emitted

* Acceptance / rejection outcomes

* Bandit statistics updating over time

* Increasing exploitation of better prices per context

Learning state is persisted in bandit_stats.json and survives restarts.

## Stopping the System

Stop any component with:

```bash
Ctrl + C
```

The bandit engine safely:

* Saves learning state

* Flushes Kafka producers

* Commits consumer offsets

Stop Kafka with:

```bash
docker compose down
```
