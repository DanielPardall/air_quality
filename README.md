# Air Quality Streaming Pipeline

**An academic data engineering project that connects air-quality measurements to a streaming pipeline and an interactive dashboard.**

The pipeline retrieves measurements from the OpenAQ API, publishes events to Apache Kafka, processes them with Spark Structured Streaming and writes the results to PostgreSQL for exploration in Grafana.

Developed by **Daniel Monteiro and Gustavo Sousa** for *Infraestruturas Avançadas para Ciência de Dados*, University of Coimbra, 2025.

## What the project demonstrates

- API ingestion with pagination, request timeouts and retries.
- A Python Kafka producer with JSON events keyed by sensor ID.
- Schema-based parsing and filtering in PySpark Structured Streaming.
- Micro-batch processing and JDBC writes to PostgreSQL.
- Temporal summaries and illustrative threshold alerts.
- A Grafana dashboard with five panels.
- Container configuration with Docker Compose and Kubernetes manifests for Minikube.

This repository is an educational prototype. Its current aggregation and recovery limitations are documented below; it does not claim production reliability or exactly-once delivery.

## Data flow

1. **OpenAQ:** retrieve measurements for three sensors associated with the Olivais station in Lisbon.
2. **Python producer:** extract the sensor, location, pollutant, unit, value and measurement timestamp, then publish to the `air_quality` Kafka topic.
3. **Spark:** parse the event schema, convert timestamps and filter null or negative measurement values.
4. **PostgreSQL:** store accepted measurements, batch-level temporal summaries and threshold alerts.
5. **Grafana:** query PostgreSQL and display trends, summaries and alerts.

The configured pollutants are carbon monoxide (`co`), nitrogen dioxide (`no2`) and ozone (`o3`). Ingestion begins at `2025-01-01T00:00:00+00:00` and then polls for newer observations every 60 seconds. Availability and measurement frequency depend on the upstream API; this is polling-based ingestion, not a direct live sensor feed.

## Stack

| Component | Role |
| --- | --- |
| Python / Requests | API ingestion |
| Apache Kafka | Event transport |
| Apache Spark / PySpark | Structured Streaming transformations |
| PostgreSQL | Storage and SQL analysis |
| Grafana | Dashboard visualizations |
| Docker Compose | Local multi-container configuration |
| Kubernetes / Minikube | Local orchestration configuration |

## Repository guide

| Path | Contents |
| --- | --- |
| [`kafka_app/kafka_code/openaq.py`](kafka_app/kafka_code/openaq.py) | OpenAQ API client, pagination and retries |
| [`kafka_app/kafka_code/producer.py`](kafka_app/kafka_code/producer.py) | Continuous ingestion and Kafka publishing |
| [`spark/spark_job.py`](spark/spark_job.py) | Streaming transformations and database writes |
| [`db/schema.sql`](db/schema.sql) | Tables and indexes |
| [`docker-compose.yaml`](docker-compose.yaml) | Local service configuration |
| [`kubernetes/`](kubernetes/) | Deployments, services and persistent storage definitions |
| [`run_kubernetes.sh`](run_kubernetes.sh) | Original cluster setup script; see the warning below |
| [`iacd_json.txt`](iacd_json.txt) | Grafana dashboard export in JSON format |

## Database outputs

| Table | Current contents |
| --- | --- |
| `air_raw` | Accepted measurements and ingestion timestamps |
| `air_daily` | Per-day statistics computed within each micro-batch |
| `air_monthly` | Per-month statistics computed within each micro-batch |
| `air_alerts` | Rows for batch/day groups whose maximum exceeds a configured threshold |

Daily and monthly statistics include mean, minimum, maximum and an observation count. The column `num_hours` currently counts records; it does not verify that each record represents a distinct hour.

The alert thresholds are project-defined demonstration values, not official air-quality standards. The code assigns `num_exceedances = 1` per emitted alert row, rather than counting all measurements above the threshold. It also does not normalize units across sensors.

## Grafana dashboard

The supplied JSON export defines five panels covering:

- Monthly pollutant averages.
- Alert distribution by pollutant.
- Monthly alert counts.
- Raw measurement time series.
- Annual pollutant statistics.

Once PostgreSQL and Grafana are running, configure a PostgreSQL data source using the supplied local settings:

| Setting | Value |
| --- | --- |
| Host inside the container network | `postgres:5432` |
| Database | `air_quality_db` |
| User | `iacd_user` |
| Password | `iacd_pass` |

These are demonstration credentials. Select the appropriate TLS setting for your environment. Import the JSON content of `iacd_json.txt` into Grafana and associate the panels with your PostgreSQL data source; exported data-source identifiers may need updating. Review the dashboard's time range against the dates actually ingested.

## Local execution

### Prerequisites

- Docker with Docker Compose.
- An OpenAQ API key and network access to the API.
- Network access to download container images, Python dependencies and Spark JDBC/Kafka connectors.

In the code-only distribution, the original API key has been replaced by `YOUR_OPENAQ_API_KEY` in `kafka_app/kafka_code/openaq.py`. Supply your own key in a private local copy before building. Do not commit the populated file or publish an image containing that key. Moving the key to an environment variable or secret is a recommended next improvement; the current code does not read it from the environment.

### Docker Compose

From the directory containing `docker-compose.yaml`:

```bash
docker compose up --build
```

**Startup caveat:** the supplied Kafka service overrides its command with a one-shot topic-creation command and does not explicitly start the broker in that command. Review and correct broker startup if Kafka exits or remains unhealthy. Topic creation should occur after a running broker becomes available. The command above is the supplied launch entry point, not a verified clean-install guarantee.

Inspect services and logs with:

```bash
docker compose ps
docker compose logs --tail=100 kafka producer spark
```

Once running, Grafana is available at <http://localhost:3000>. The supplied local configuration uses `admin` / `admin`; change the password when prompted. Configure and import the dashboard as described above.

To stop services while retaining named volumes:

```bash
docker compose down
```

### Kubernetes / Minikube

The `kubernetes/` directory provides a Kafka StatefulSet, Deployments for the other services, Services and persistent volume claims. Each workload is configured with one replica; this is a local learning environment, not a high-availability cluster.

> **Do not run `run_kubernetes.sh` without reviewing it.** The original script deletes the default Minikube cluster and executes Docker image/container and volume pruning commands. This can remove unrelated local resources.

A manual setup that avoids those cleanup commands, in a Bash-compatible shell with Docker, Minikube and kubectl installed:

```bash
minikube start --driver=docker
eval "$(minikube docker-env)"
docker build -t sql-postgres:latest ./db
docker build -t kafka-producer:latest ./kafka_app
docker build -t spark-job:latest ./spark
kubectl create namespace project --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n project -f kubernetes/
kubectl get pods,pvc -n project
```

Inspect pod status and logs, and ensure the `air_quality` topic exists before expecting data to flow. The supplied producer and Spark manifests do not include readiness-waiting init containers. Configuration should be validated before treating this as a reproducible deployment.

Once Grafana is ready:

```bash
kubectl port-forward -n project svc/grafana 3000:3000
```

## Known limitations and improvements

- **Temporal aggregation:** daily/monthly results are computed inside `foreachBatch` and appended. Separate batches for the same period produce partial rows, not an updated consolidated aggregate. A period-level upsert or a suitable stateful aggregation strategy is needed.
- **Duplicate handling:** the producer cursor exists only in memory and resets on restart. Database writes append without uniqueness constraints for measurements or idempotency by batch ID. Retries and replay can create duplicates.
- **Checkpoint persistence:** Spark uses `checkpoints/air_quality_postgres`, but the supplied container configuration does not mount persistent storage for that checkpoint.
- **Validation:** negative and null values are filtered, but full schema validation, timestamp validation and unit normalization are not implemented.
- **Configuration:** some settings are hard-coded in Python even though similarly named environment variables appear in the deployment files.
- **Availability:** the producer can stop after exhausting API retries; single-replica services and current restart behavior do not establish high availability.
- **Dependencies:** some image tags and Python dependencies are not pinned. Reproducibility requires validating and fixing the complete environment.

## Verification status

This README was prepared by reviewing the supplied source, deployment files and dashboard export. The complete stack was not executed as part of this documentation pass. No automated test suite or measured throughput benchmark is included.

## Authors

- Daniel Monteiro
- Gustavo Sousa
