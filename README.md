# IoT Sensor Streaming Platform with Flink and InfluxDB

## 1. Overview
A high-throughput, stateful stream processing platform designed to ingest telemetry from 100K+ IoT devices (factory floor sensors, fleet vehicles) at a peak rate of ~1 million events per second. The system performs real-time validation, enrichment, stateful anomaly detection (using Flink CEP and statistical models), sliding/tumbling aggregations, and dual-sink storage: operational time-series hot storage in InfluxDB and long-term analytical cold storage in Apache Iceberg on AWS S3/MinIO. It triggers sub-second latency operational alerts routed through PagerDuty and Slack with Redis-based storm suppression.

## 2. Business Use Case
Industrial manufacturing plants operate thousands of sensors monitoring physical parameters (temperature, pressure, vibration RMS, RPM) to detect mechanical wear and prevent catastrophic failures. WAN network connectivity between factory edge gateways and centralized clouds is notoriously unstable. Plant managers require:
1. **Real-time visibility** via low-latency dashboards.
2. **Sub-second predictive alerts** to execute automated machine shutdowns before mechanical failures.
3. **Historical analytics and ML models** to perform predictive maintenance forecasting.

## 3. High-Level System Architecture
```
                                        +-------------------+
                                        |  PostgreSQL DB    |
                                        | (Device Registry) |
                                        +---------+---------+
                                                  |
                                                  v (Debezium CDC)
+-----------------------+   MQTT / mTLS +---------v---------+   Kafka Source    +-------------------+
|  100K+ Edge Sensors   +-------------->| EMQX Broker Cluster+-------------->| Apache Kafka      |
|  (Modbus/OPC-UA)      |               +-------------------+                 | (raw-telemetry)   |
+-----------+-----------+                                                     +---------+---------+
            | (WAN Down)                                                                |
            v                                                                           v
+-----------v-----------+                                                     +---------v---------+
| Local RocksDB Buffer  |                                                     | Apache Flink      |
+-----------------------+                                                     | (Stream Processor)|
                                                                              +----+----+----+----+
                                                                                   |    |    |
                                      +------------------------+-------------------+    |    +-------------------------+
                                      | (Hot Time-Series Stream)                        | (Stateful CEP / Alerts)      | (Cold Analytical Lakehouse)
                                      v                                                 v                              v
                              +-------v-------+                                 +-------v-------+              +-------v-------+
                              | InfluxDB v2   |                                 | Kafka Alerts  |              | Apache Iceberg|
                              +-------+-------+                                 +-------+-------+              |   (S3/MinIO)  |
                                      |                                                 |                      +-------+-------+
                                      v (Grafana Views)                                 v (FastAPI Deduplicator)       |
                              +-------v-------+                                 +-------v-------+              v (PySpark ML)
                              | Grafana Board |                                 | PagerDuty /   |      +-------v-------+
                              +---------------+                                 | Slack Alert   |      | ML Score Batch|
                                                                                +---------------+      +---------------+
```

## 4. Technology Stack
* **EMQX Enterprise (5.1)**: IoT-native MQTT broker behind a Network Load Balancer (NLB).
* **Apache Kafka (3.6)**: Internal event backbone for raw telemetry and change-data-capture (CDC).
* **Apache Flink (1.18)**: Stateful stream processing (DataStream, Flink CEP, Temporal Joins).
* **Debezium CDC**: Captures Postgres device metadata updates in real-time.
* **InfluxDB (2.7)**: Time-series database for hot dashboards.
* **Apache Iceberg + MinIO/S3**: Long-term Parquet-backed data lakehouse.
* **Redis (7.0)**: Device state cache, active outage map, and idempotency alert engine.
* **FastAPI (Python 3.10)**: Microservices for alert deduplication and ML serving.
* **PySpark**: Hourly batch jobs for Parquet compaction and predictive scoring.
* **Terraform & Helm**: Infrastructure-as-code and K8s orchestration.

## 5. Directory Layout
* `edge/`: Contains 100K device telemetry simulators and WAN-resilient RocksDB-backed local queues.
* `emqx/`: Rules engine definitions translating MQTT hierarchies into flat Kafka keys.
* `postgres/`: DDL schemas for organizations, sites, assets, planned outages, and replication setups.
* `flink-jobs/`: Core Flink Maven project implementing enrichment, CEP anomalies, and windowing.
* `alert-service/`: FastAPI application utilizing Redis pipelines with sliding-window `SETNX` deduplication.
* `ml-pipeline/`: PySpark hourly compaction script (`rewrite_data_files`) and predictive batch models.
* `terraform/`: HCL infrastructure modules for AWS EKS, MSK, and Managed Flink deployments.
* `grafana/`: Operational dashboard JSONs and datasource provisioning configurations.

## 6. Getting Started (Sandbox Setup)
Launch the complete data engineering sandbox environment containing all systems:
```bash
docker-compose up -d --build
```
Verify compilation of the Flink streaming engine:
```bash
cd flink-jobs
mvn clean compile package
```

Refer to the sub-folder READMEs for detailed API specifications and deployment blueprints.
