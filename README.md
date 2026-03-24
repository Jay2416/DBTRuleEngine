# Spark Iceberg DBT Rule Engine

A Dockerized data platform for building, reviewing, approving, and executing dbt-powered rule logic on Spark Thrift with Iceberg tables stored in MinIO.

## What This Repository Contains

- Spark cluster (master + workers)
- Spark Thrift Server for SQL and dbt connectivity
- MinIO object storage for Iceberg warehouse data
- dbt runtime container for model execution and debugging
- Streamlit application for rule creation, approval workflow, and execution
- Dremio for exploration and query access

## High-Level Architecture

- Storage layer: MinIO buckets hold Iceberg metadata and data files.
- Compute layer: Spark + Iceberg extensions process SQL and model logic.
- SQL endpoint: Spark Thrift listens on port 10000 for dbt and app queries.
- Transformation layer: dbt project under dbt/formula1.
- Application layer: Streamlit UI for rule authoring, approval, and orchestration.

## Services and Ports

| Service | Container | Purpose | Host Port |
|---|---|---|---|
| Spark Master | spark-master | Cluster coordinator | 7077, 8080 |
| Spark Worker 1 | spark-worker-1 | Spark execution | - |
| Spark Worker 2 | spark-worker-2 | Spark execution | - |
| Spark Thrift | spark-thrift | JDBC/Thrift SQL endpoint | 10000, 4040 |
| MinIO API | minio | Object storage API | 9100 |
| MinIO Console | minio | Object storage UI | 9001 |
| Dremio UI | dremio | Query and semantic access | 9047 |
| Dremio Flight/Client | dremio | Client connectivity | 31010 |
| Streamlit UI | streamlit-ui | Rule manager app | 8501 |
| dbt Runtime | dbt | dbt commands | - |

## Core Connection Configuration

### Spark Thrift

- Host: spark-thrift
- Port: 10000
- Transport mode: binary
- Bound host: 0.0.0.0

### dbt Profile (formula1)

- type: spark
- method: thrift
- host: spark-thrift
- port: 10000
- schema: default
- threads: 4

### Spark Catalogs

Spark Thrift starts with multiple Iceberg catalogs:

- school_catalog -> s3a://cygnet/iceberg_data/
- netflix_catalog -> s3a://netflix/iceberg_data/
- formula1_catalog -> s3a://formula1/iceberg_data/

Default catalog is formula1_catalog.

### MinIO

- Endpoint inside Docker network: http://minio:9000
- Access key: admin
- Secret key: password123
- Path style access: true
- SSL enabled: false

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- At least 6 GB free memory for containers
- Ports listed above available on the host

## Quick Start

### 1. Start all services

```bash
docker compose up -d --build
```

### 2. Verify service status

```bash
docker compose ps
```

### 3. Open web interfaces

- Streamlit: http://localhost:8501
- Spark Master: http://localhost:8080
- MinIO Console: http://localhost:9001
- Dremio: http://localhost:9047

## Namespace and Environment Bootstrap

Run this once after services are up, or whenever you reset volumes.

### 1. Open Spark SQL inside the Thrift container

```bash
docker compose exec spark-thrift /opt/spark/bin/spark-sql
```

### 2. Create required namespaces for formula1 catalog

```sql
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.default;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.staging;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.rules;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_rules;

CREATE NAMESPACE IF NOT EXISTS formula1_catalog.driver_test;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.testing_ns;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.test_group;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.front_grid_starters;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.driver_podium_history;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.yearly_race_calendar;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.nationality_driver_aggregation;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.driver_flags_ns;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.driver_badges_list;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.all_time_wins_leaderboard;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.testing_group;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.championship_standings_2023;

CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_front_grid_starters;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_driver_podium_history;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_yearly_race_calendar;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_nationality_driver_aggregation;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_driver_flags_ns;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_driver_badges_list;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_all_time_wins_leaderboard;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_testing_group;
CREATE NAMESPACE IF NOT EXISTS formula1_catalog.final_championship_standings_2023;
```

### 3. Verify namespaces were created

```sql
SHOW NAMESPACES IN formula1_catalog;
```

### 4. Verify Spark can read the catalog

```sql
SHOW TABLES IN formula1_catalog.default;
```

### 5. Exit Spark SQL

```sql
exit;
```

### 6. Verify MinIO buckets and warehouse paths

1. Open MinIO Console at http://localhost:9001.
2. Confirm buckets exist: formula1, netflix, cygnet.
3. Confirm Iceberg warehouse path is available under each bucket as iceberg_data/.
4. If missing, create the bucket and rerun dbt debug before dbt run.

## dbt Workflow (Including dbt Debug for Thrift Connectivity)

Run these commands from the repository root.

### 1. Open dbt container shell

```bash
docker compose exec dbt bash
```

### 2. Move to dbt project

```bash
cd /usr/app/dbt/formula1
```

### 3. Validate dbt-to-Spark Thrift connection

Use the project-local profile file:

```bash
dbt debug --project-dir /usr/app/dbt/formula1 --profiles-dir /usr/app/dbt/formula1
```

If you use mounted default profile location (/root/.dbt), use:

```bash
dbt debug --project-dir /usr/app/dbt/formula1
```

### 4. Install dependencies (if required)

```bash
dbt deps --project-dir /usr/app/dbt/formula1 --profiles-dir /usr/app/dbt/formula1
```

### 5. Run models

```bash
dbt run --project-dir /usr/app/dbt/formula1 --profiles-dir /usr/app/dbt/formula1
```

### 6. Run tests

```bash
dbt test --project-dir /usr/app/dbt/formula1 --profiles-dir /usr/app/dbt/formula1
```

## Streamlit Rule Engine Workflow

1. Login to the Streamlit app.
2. Create/select a rule group.
3. Build rule SQL in the query builder.
4. Save rule to workflow and DBT model location.
5. Approvals progress through levels (Manager -> Technical -> SME).
6. Once fully approved, group-level execution is triggered.

### Sequential vs Non-Sequential

- Sequential mode: order-sensitive rule execution.
- Non-sequential mode: rules run in parallel and aggregate output by a configured key with remark output.

## Useful Operational Commands

### Follow logs

```bash
docker compose logs -f streamlit-ui
docker compose logs -f spark-thrift
docker compose logs -f dbt
```

### Restart a specific service

```bash
docker compose restart streamlit-ui
```

### Stop everything

```bash
docker compose down
```

### Stop and remove volumes

```bash
docker compose down -v
```

## Troubleshooting

### dbt debug fails to connect to Thrift

- Ensure spark-thrift container is running and healthy:
  - docker compose ps
  - docker compose logs spark-thrift
- Confirm dbt profile uses host spark-thrift and port 10000.
- Re-run dbt debug with explicit project and profiles paths.
- Restart services in order:
  - spark-master, workers, spark-thrift, then dbt

### Streamlit UI cannot query data

- Confirm spark-thrift and minio are running.
- Check streamlit logs:
  - docker compose logs -f streamlit-ui
- Verify app connection constants still target spark-thrift:10000.

### Iceberg write/read errors

- Validate S3A settings and credentials in Spark startup configs.
- Confirm expected MinIO bucket exists and is accessible.
- Check Spark Thrift logs for catalog/warehouse path errors.

### Hot reload not reflecting local edits

- streamlit-ui is configured with polling watcher.
- If changes still do not appear, restart streamlit-ui.

## Repository Layout

- docker-compose.yml -> service orchestration
- Dockerfile.spark -> Spark runtime with Iceberg and AWS jars
- spark-defaults.conf -> Spark defaults
- dbt/formula1 -> main dbt project
- streamlit_app -> rule management and approval UI
- dremio_data -> persisted Dremio state
- .dbt -> optional mounted dbt profiles

## Notes

- Credentials in this repository are development defaults and should be replaced in real environments.
- Keep service names unchanged unless you also update dbt profiles and app connection constants.
