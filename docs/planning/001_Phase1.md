# Phase 1: The "Hello World" Stream

1. Spin up **Redpanda** using Docker Desktop.
2. Write a Python script that connects to the **Coinbase Pro WebSocket** (or Binance) for `BTC-USD` tickers and prints them to your console.
3. Modify that script to route those JSON messages directly into a Redpanda topic named `raw-crypto-trades`.

## Redpanda

- Follow the documentation [https://docs.redpanda.com/labs/docker-compose/single-broker/]

### Kafka Alternative

Simples Kafka (single-node server)
```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      # KRaft mode settings
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:29093,PLAINTEXT_HOST://:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      #CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk' # Standard ID for local testing
```