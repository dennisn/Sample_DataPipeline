# Phase 2: The Processing Layer

1. Write a consumer script (using Flink or Faust).
2. Read from `crypto_trades`.
3. Implement a **Tumbling Window** (e.g., group the data into strict 1-minute blocks) and calculate the Volume Weighted Average Price (VWAP) for that minute.
4. Stream those calculated metrics into a new topic called `processed-crypto-metrics`.

