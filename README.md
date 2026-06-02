# Sample_DataPipeline

A sample datapipeline project, as details in [overal plan](./docs/planning/000_OveralPlan.md)

1. [Phase 1](./docs/planning/001_Phase1.md)

## Setup

### Pre-requisites

- Docker installed
- Python & environment:
  
  ```powershell
  # From the project root
  python -m virtualenv .venv
  .venv/Scripts/activate.ps1
  python -m pip install -r requirements.txt
  ```

### Starting Kafka (RedPanda)

```powershell
# Change to redpanda folder
cd container/redpanda

# start up the docker (to shut down)
docker compose up -d
```

### Data Extraction

- Run the `stock_service.py`

  ```powershell
  python src/stock_service.py
  ```