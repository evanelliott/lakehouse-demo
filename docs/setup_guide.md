# 🛠️ Lakehouse Demo: Setup Guide (macOS M4)
This guide ensures a clean deployment of the Lakehouse stack on Apple Silicon (M4) with 16GB RAM.

## 1. Prerequisites
### Hardware & OS
- Machine: Mac Mini M4 (16GB RAM)
- Architecture: ARM64 (Apple Silicon)
### Software
- Docker Desktop: Ensure 'VirtioFS' file sharing is enabled for M4 performance.
- Python 3.10+: For running local orchestration scripts.
- Make: Standard on macOS (via Xcode Command Line Tools).

## 2. Docker Resource Allocation (M4 Optimised)
On a 16GB machine, preventing "Memory Pressure" is critical. Configure Docker Desktop (Settings > Resources):
- CPUs: 6
- Memory: 12 GB
- Swap: 2 GB
- Virtualization: Use Apple Virtualization Framework

## 3. Initial Deployment Flow
### Initialize Environment:
`cp .env.example .env`
### Start the Engines:
`make up`
(Note: The initial build of the custom Airflow image takes ~2 mins)
### Verify Health:
`make health`
(Wait until all services return 'OK' before proceeding)

## 4. The Data Bootstrap Sequence
Run these commands in order to populate the Lakehouse:
1. Seed Metadata: make seed (Creates Iceberg tables/namespaces).
2. Generate Data: make mock-data (Ingests raw football events).
3. Trigger ETL: Access Airflow at http://localhost:8085 and unpause the DAG.

## 5. Troubleshooting for Senior DEs
- Trino OOM: If Trino crashes (Exit 137), check if other heavy apps (Chrome, Slack) are hogging RAM. M4 Unified Memory is shared with the GPU.
- Port Conflicts: If 8080 is taken, update TRINO_PORT in .env.
- Permission Errors: If Airflow cannot write logs to the volume, run chmod -R 777 airflow/logs.

## 6. Access Matrix
- Trino UI: http://localhost:8080
- MinIO: http://localhost:9001
- Airflow: http://localhost:8085
- Jupyter: http://localhost:8888 (Token: lakehouse)
