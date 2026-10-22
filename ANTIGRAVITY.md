# ANTIGRAVITY.md - Project Guide & AI Context

## 1. Project Overview & Architecture
This project is an end-to-end data engineering platform designed to process real-time hydrological data from the **USGS National Water Information System (NWIS)** using **Apache Kafka**, **Apache Spark**, **Hadoop HDFS**, and **PostgreSQL**, orchestrated by a **Spring Boot 3.2.4** application.

### Architecture Flow
```
[USGS NWIS API]
       │
       ▼ (HTTP GET JSON - parameterCd=00060 / streamflow)
[UsgsDataIngestionService] (Spring Scheduled Producer)
       │
       ▼ (Kafka Topic: `usgs-water-metrics`)
┌──────┴──────────────────────────┐
│                                 │
▼ (Spark Structured Streaming)     ▼ (Spark Batch Sync)
[SparkStreamingProcessor]         [SparkBatchProcessor]
│                                 │
▼ (JDBC Sink)                     ▼ (Append JSON / Parquet)
[PostgreSQL: reporting_db]        [Hadoop HDFS: hdfs://namenode:9000]
│
▼ (Spring Data JPA)
[ReportingController: /api/reports/latest-water-metrics]
```

---

## 2. Infrastructure & Prerequisites

### Toolchain & Runtime Requirements
- **OS**: macOS (Apple Silicon `aarch64` / `arm64`).
- **Java**: OpenJDK 17 (`JAVA_HOME` pointing to Java 17).
- **Gradle**: 8.7 (via `./gradlew`).
- **Container Engine**: Colima + Docker Compose.

### Starting Local Infrastructure
Colima runs the Docker daemon on macOS.
```bash
# 1. Start Colima with sufficient CPU & Memory for Spark + Hadoop + Kafka + Postgres
colima start --cpu 4 --memory 8

# 2. Launch container ecosystem in background
docker compose up -d

# 3. Verify services are healthy
docker compose ps
```

### Port Map
| Service | Container Name | Host Port | Internal Port | Description |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `pipeline-postgres` | `5432` | `5432` | Database (`reporting_db`, `pipeline_user`) |
| **Kafka (KRaft)** | `pipeline-kafka` | `9092` | `9092` (Host), `29092` (Internal) | KRaft broker & controller |
| **HDFS NameNode** | `pipeline-namenode` | `9870`, `9000` | `9870` (Web UI), `9000` (RPC) | HDFS Master / Metadata |
| **HDFS DataNode** | `pipeline-datanode` | `9864`, `9866` | `9864` (HTTP), `9866` (Data Transfer) | HDFS Storage Block Server |
| **Spark Master** | `pipeline-spark-master` | `8080`, `7077` | `8080` (Web UI), `7077` (Cluster RPC) | Spark Standalone Master |
| **Spark Worker** | `pipeline-spark-worker` | `8081` (Internal) | dynamic | Spark Standalone Worker |
| **Spring Boot App** | Host process | `8081` | `8081` | REST APIs & Ingestion Runner |

---

## 3. Build, Run & Test Commands

### Gradle Tasks
```bash
# Compile code
./gradlew compileJava

# Run Spring Boot application (includes all required Java 17 reflection flags for Spark)
./gradlew bootRun

# Run unit & integration tests
./gradlew test
```

### AI Agent Execution Notes
- When executing Gradle inside restricted environments or sandboxes where daemon IPC over loopback sockets is restricted, Gradle may require `--no-daemon` or elevated execution permissions.
- Java 17 requires specific JVM `--add-opens` flags for Apache Spark internals. These are pre-configured in `build.gradle` for `bootRun` and should be mirrored on `test` tasks.

---

## 4. Key Components & Implementation Details

### 1. Ingestion (`com.pipeline.ingestion.UsgsDataIngestionService`)
- Source URL: `https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd=va&parameterCd=00060`
- Format: USGS WaterML 2.0 JSON. The payload contains `value.timeSeries[]`.
- **Parsing Requirement**: The raw payload is a nested ~400KB bundle containing ~274 Virginia gauging stations. The ingestion service extracts discrete records (`siteCode`, `siteName`, `value`, `dateTime`) and publishes individual messages to Kafka keyed by `siteCode`.

### 2. Kafka Configuration (`com.pipeline.config.KafkaConfig`)
- Defines `NewTopic` bean for `usgs-water-metrics` (partitions: 3, replicas: 1).
- Provides `ProducerFactory` and `ConsumerFactory` configurations.

### 3. Stream Processing (`com.pipeline.service.SparkStreamingProcessor`)
- Spark Structured Streaming job consuming from Kafka topic `usgs-water-metrics`.
- Parses incoming JSON events into schema:
  - `siteCode` (String)
  - `timestamp` (Timestamp)
  - `value` (Double - streamflow in $ft^3/s$)
- Sinks to PostgreSQL `water_metrics` table using `.foreachBatch(...)` JDBC append mode.

### 4. Batch Processing (`com.pipeline.processing.SparkBatchProcessor`)
- Batch extraction from Kafka.
- Writes raw/curated partitions directly into Hadoop HDFS (`hdfs://localhost:9000/data/usgs_water_metrics.json` or Parquet).

### 5. Reporting API (`com.pipeline.reporting.ReportingController`)
- `GET /api/reports/latest-water-metrics`: Returns top 10 most recent readings from PostgreSQL.

---

## 5. Critical Gotchas & Troubleshooting

1. **HDFS DataNode Resolution from macOS Host**:
   - When Spark running on the host connects to `hdfs://localhost:9000`, the NameNode directs the client to the DataNode.
   - For host-to-container communication to succeed:
     1. `docker-compose.yml` must expose DataNode port `9866` (data transfer) and `9864` (web/HTTP).
     2. Add `127.0.0.1 datanode` to `/etc/hosts` if NameNode returns internal container hostnames, or set `dfs.client.use.datanode.hostname=true` in HDFS client configuration.

2. **Spark Classpath & Logging Conflicts**:
   - Spring Boot 3 uses Jakarta EE (`jakarta.servlet.*`), whereas Spark 3.5.x still relies on `javax.servlet.*` for internal metrics UI.
   - `build.gradle` resolves this via `implementation 'javax.servlet:javax.servlet-api:4.0.1'` and explicit logging exclusions (`log4j-slf4j2-impl`, `slf4j-log4j12`).

3. **Kafka KRaft Dual Listeners**:
   - `docker-compose.yml` defines `PLAINTEXT://kafka:29092` for container-to-container traffic and `PLAINTEXT_HOST://localhost:9092` for the host Spring Boot process. Do not point the host app to port 29092.

4. **PostgreSQL Schema Auto-DDL**:
   - Spring Boot runs with `spring.jpa.hibernate.ddl-auto: update`. Let Spring Boot create the `water_metrics` table before Spark writes, or ensure table initialization scripts match the entity schema.
