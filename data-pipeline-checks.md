# Data Pipeline Verification Guide: Hadoop & Kafka

> **Question:** How to check Hadoop and Kafka?

---

## 1. Container Status & Ecosystem Overview

Before inspecting individual components, verify that all Docker containers in the ecosystem are running:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Expected running containers:
* `pipeline-kafka` (Apache Kafka 3.7.0 in KRaft mode)
* `pipeline-namenode` (Hadoop 3.3.6 HDFS NameNode)
* `pipeline-datanode` (Hadoop 3.3.6 HDFS DataNode)
* `pipeline-postgres` (PostgreSQL 16)
* `pipeline-spark-master` & `pipeline-spark-worker` (Apache Spark 3.5.1)

---

## 2. How to Check Kafka

Kafka runs in the `pipeline-kafka` container on port `9092` (host) / `29092` (container internal).

### A. List Topics
Verify that the `usgs-water-metrics` topic is present:
```bash
docker exec pipeline-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### B. Describe Topic Details (Partitions, Replicas, ISR)
Inspect partition count, replication factor, and in-sync replicas:
```bash
docker exec pipeline-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic usgs-water-metrics
```

### C. Read / Tail Messages
Inspect actual USGS hydrological JSON records:

* **Read the first message from the beginning:**
  ```bash
  docker exec pipeline-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic usgs-water-metrics \
    --from-beginning \
    --max-messages 1
  ```

* **Tail live incoming stream in real time** (press `Ctrl+C` to stop):
  ```bash
  docker exec -it pipeline-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic usgs-water-metrics
  ```

### D. Check Consumer Groups & Processing Lag
Monitor offsets, consumer progress, and consumer lag:
```bash
# List all active consumer groups
docker exec pipeline-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list

# Describe offsets and lag for the Spring Boot / Spark consumer group
docker exec pipeline-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group pipeline-group
```

### E. View Kafka Broker Logs
```bash
docker logs pipeline-kafka --tail 50 -f
```

---

## 3. How to Check Hadoop HDFS

Hadoop HDFS consists of the **NameNode** (`pipeline-namenode`) and **DataNode** (`pipeline-datanode`).

### A. HDFS Web UI (Browser)
Open your web browser to:
👉 **[http://localhost:9870](http://localhost:9870)**

Features available in the Web UI:
* **Overview:** Check DFS storage capacity, used space, and remaining storage.
* **Datanodes Tab:** Verify that the DataNode (`pipeline-datanode`) is registered and in healthy operation.
* **Utilities &rarr; Browse the file system:** Interactively navigate directories and download/view files in HDFS.

### B. Check Cluster Health & DataNode Report (CLI)
View total cluster capacity, block count, and connected DataNodes:
```bash
docker exec pipeline-namenode /opt/hadoop/bin/hdfs dfsadmin -report
```
*Look for:* `Live datanodes (1)` with `Decommission Status: Normal`.

### C. Browse Files and Directories
Use standard HDFS file-system commands (`hdfs dfs`):

```bash
# List files at root
docker exec pipeline-namenode /opt/hadoop/bin/hdfs dfs -ls /

# Recursively list files in /data directory
docker exec pipeline-namenode /opt/hadoop/bin/hdfs dfs -ls -R /data
```

### D. Read or Preview Files Stored in HDFS
```bash
# Print contents of batch-synced JSON files in HDFS
docker exec pipeline-namenode /opt/hadoop/bin/hdfs dfs -cat /data/usgs_water_metrics/*.json | head -n 10
```

### E. Run File System Consistency Check (`fsck`)
Ensure there are no missing or corrupt blocks:
```bash
docker exec pipeline-namenode /opt/hadoop/bin/hdfs fsck / -files -blocks
```

### F. View Hadoop Logs
```bash
# NameNode logs
docker logs pipeline-namenode --tail 50 -f

# DataNode logs
docker logs pipeline-datanode --tail 50 -f
```

---

## 4. End-to-End Pipeline Verification Workflow

When the Spring Boot application is running (`./gradlew bootRun`), verify data movement across Kafka, Spark, PostgreSQL, and HDFS:

```bash
# 1. Trigger an immediate ingestion from USGS NWIS API into Kafka
curl -s -X POST http://localhost:8081/api/reports/trigger-ingest

# 2. Check total records written to PostgreSQL via Spark Structured Streaming
curl -s http://localhost:8081/api/reports/status

# 3. Retrieve the latest 10 hydrological streamflow readings
curl -s http://localhost:8081/api/reports/latest-water-metrics

# 4. Trigger Spark batch synchronization to Hadoop HDFS
curl -s -X POST http://localhost:8081/api/reports/sync-to-hdfs

# 5. Verify batch files in HDFS
docker exec pipeline-namenode /opt/hadoop/bin/hdfs dfs -ls /data
```
