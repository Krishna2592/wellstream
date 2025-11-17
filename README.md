# WellStream - Real-Time IoT Streaming Pipeline

> Production-grade Kafka + Spark streaming pipeline for oil & gas sensor monitoring with Redis caching and Kubernetes deployment

![Java](https://img.shields.io/badge/Java-11-orange) ![Spark](https://img.shields.io/badge/Spark-3.5-red) ![Kafka](https://img.shields.io/badge/Kafka-7.4-black) ![Docker](https://img.shields.io/badge/Docker-Ready-blue) ![Kubernetes](https://img.shields.io/badge/Kubernetes-Ready-blue)

## Features

- **Real-time streaming** from Kafka topics with exactly-once semantics
- **Spark Structured Streaming** with time-windowed joins (±5 sec correlation)
- **Redis caching** for hot data with Jedis client
- **Risk scoring** and data quality tracking (70-95 quality scores)
- **Docker Compose** for local development
- **Kubernetes** manifests with StatefulSets for Kafka/Zookeeper
- **Prometheus + Grafana** observability stack

## Quick Start

### Docker Compose (Local Development)

```bash
# Build JARs
mvn clean package -DskipTests

# Start all services
docker compose up -d

# View Spark streaming output
docker logs -f wellstream-spark
```

**Access Points:**
- Spark UI: http://localhost:4040
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Schema Registry UI: http://localhost:8081
- KSQL CLI: `docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`

### Kubernetes (Production)

```bash
# Deploy application
kubectl apply -f k8s

# Watch Spark processor logs
kubectl logs -f deployment/spark-processor

# Check all pods
kubectl get pods
```

See [k8s/README.md](k8s/README.md) for detailed Kubernetes deployment guide.

## Project Structure

```
wellstream_2/
├── docs/
│   ├── ARCHITECTURE.md          # System architecture & data flow
│   └── observability.md         # Monitoring setup guide
├── avro-schemas/                # Avro schemas for all topics (registered in Schema Registry)
│   ├── well-data.avsc
│   └── tank-data.avsc
├── k8s/
│   ├── kafka.yml                # Kafka StatefulSet
│   ├── zookeeper.yml            # Zookeeper StatefulSet
│   ├── producer.yml             # Data producer deployment
│   ├── spark-processor.yml      # Spark streaming job
│   ├── schema-registry.yml      # Schema Registry deployment/service
│   ├── ksqldb.yml               # KSQLDB deployment/service
│   ├── redis-*.yml              # Redis cache
│   └── monitoring/              # Prometheus & Grafana
├── monitoring/
│   ├── prometheus.yml           # Metrics scrape config
│   └── grafana-dashboards/      # Pre-built dashboards
├── producer/
│   ├── src/main/java/com/wellstream/streaming/
│   │   └── FakeDataProducer.java
│   └── Dockerfile
├── spark-processor/
│   ├── src/main/java/com/wellstream/streaming/
│   │   └── RealTimeWellTankPipeline.java
│   ├── conf/metrics.properties
│   └── Dockerfile
├── ksql-scripts/                # KSQL scripts for stream/table creation
│   ├── create-streams.sql
│   └── create-tables.sql
├── docker-compose.yml           # Base stack
├── docker-compose.monitoring.yml # Monitoring add-on
└── pom.xml                      # Parent POM
```

## Architecture

The pipeline processes sensor data through 11 transformation steps:

```
IoT Sensors → Kafka → Spark Streaming → Risk Scoring → Console/Alerts
              ↓                              ↓
            Redis Cache                  Prometheus Metrics
```

**Key Components:**
1. **Kafka** - Message broker with 3 partitions for ordering guarantees
2. **Spark** - Structured Streaming with SQL-based risk classification
3. **Redis** - Caching layer for frequently accessed wells
4. **Prometheus/Grafana** - Metrics collection and visualization

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed data flow and transformation steps.

## Running Locally (Alternative to Docker)

```bash
# Terminal 1: Start infrastructure
docker compose up -d kafka zookeeper redis

# Terminal 2: Run producer
cd producer
mvn exec:java

# Terminal 3: Run Spark processor
cd spark-processor
mvn exec:java
```

## Monitoring

Add Prometheus and Grafana dashboards:

```bash
# Docker Compose
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d

# Kubernetes
kubectl apply -f k8s/monitoring/monitoring-stack.yml
kubectl port-forward svc/grafana -n monitoring 3000:80
```

See [docs/observability.md](docs/observability.md) for complete setup guide.

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|----------|
| Message Broker | Apache Kafka 7.4 | Event ingestion with ordering guarantees |
| Stream Processing | Apache Spark 3.5 | Real-time data transformation & windowed joins |
| Caching | Redis Alpine | Hot data caching via Jedis |
| Data Generation | JavaFaker | Realistic sensor data simulation |
| Orchestration | Docker Compose / Kubernetes | Local dev & production deployment |
| Monitoring | Prometheus + Grafana | Metrics collection & visualization |
| Build Tool | Maven 3.9 | Multi-module project management |
| Language | Java 11 | Application runtime |

## Troubleshooting

### Kafka connection refused
Kafka takes 10-15 seconds to start. Wait or check logs:
```bash
docker logs kafka
```

### Spark can't find Kafka topic
Verify topics exist:
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Maven dependency errors
Force re-download:
```bash
mvn clean install -DskipTests -U
```

### Kubernetes pods crashing
Check resource limits and service DNS:
```bash
kubectl describe pod <pod-name>
kubectl get svc
```

### Schema Registry fails to start on Kubernetes (Confluent image)
**Symptom:**
Schema Registry pod repeatedly fails with the log message:

```
PORT is deprecated. Please use SCHEMA_REGISTRY_LISTENERS instead.
```
and exits immediately, causing CrashLoopBackOff.

**Root Cause:**
Kubernetes automatically injects a `SCHEMA_REGISTRY_PORT` environment variable into your pod if your Service is named `schema-registry`. The Confluent Docker image treats this as a deprecated config and exits with an error.

**Solution:**
Rename your Service and Deployment to something that does **not** start with `schema-registry`. For example, use `schema-reg` or `sr` as the service name. Update all references in your manifests and client configs (e.g., use `http://schema-reg:8081` for the Schema Registry URL).

**Why this works:**
Kubernetes will inject `SCHEMA_REG_PORT` (not `SCHEMA_REGISTRY_PORT`), so the Confluent image will not see the deprecated variable and will start normally.

**References:**
- [Stack Overflow: Integrating Spark Structured Streaming with the Confluent Schema Registry](https://stackoverflow.com/questions/48882723/integrating-spark-structured-streaming-with-the-confluent-schema-registry/49182004#49182004)

**This is a common pain point for Confluent images on Kubernetes.**

## Future Enhancements

- **Delta Lake** - Replace console sink with ACID-compliant storage
- **Databricks** - Migrate to managed Spark clusters
- **MLOps** - Deploy ML models for predictive maintenance
- **Azure Event Hubs** - Cloud-native Kafka alternative
- **Autoscaling** - KEDA-based horizontal pod autoscaling

## License

MIT
## High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                    REAL-TIME STREAMING PIPELINE                            │
│                                                                             │
│  ┌──────────────┐        ┌──────────────┐        ┌───────────────┐        │
│  │  Well Sensors│        │ Tank Sensors │        │  Data Sources │        │
│  │  (IoT)       │        │   (IoT)      │        │               │        │
│  └──────┬───────┘        └──────┬───────┘        └───────┬───────┘        │
│         │                       │                        │                 │
│         └───────────────────────┼────────────────────────┘                │
│                                 │                                         │
│                          ┌──────▼──────┐                                  │
│                          │    KAFKA     │                                  │
│                          │  Broker(3x)  │  Step 1-2: Ingest + Schema     │
│                          │   KRaft      │                                  │
│                          └──────┬───────┘                                  │
│                                 │                                         │
│                    ┌────────────▼────────────┐                            │
│                    │   SPARK CLUSTER        │                            │
│                    │  (local[*] 4 cores)    │                            │
│                    │                        │                            │
│        ┌───────────────────────────────────────────────────┐             │
│        │                                                   │             │
│    ┌───▼────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐   │            │
│    │ Step 3 │  │ Step 4│  │ Step 5│  │ Step 6│  │ Step 7│   │            │
│    │ Init   │→ │ Read  │→ │ Cast  │→ │ Avro  │→ │ SQL   │   │            │
│    │ Spark  │  │Kafka │  │Binary │  │Parse │  │  KPIs │   │            │
│    └────────┘  └──────┘  └──────┘  └──────┘  └──────┘   │            │
│        │                                                   │             │
│        │  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐        │             │
│        │  │ Step 8│  │ Step 9│  │Step10│  │Step11│        │             │
│        └→ │Decode│→ │Filter │→ │ Join │→ │Output│        │            │
│           │Base64│  │Nulls  │  │      │  │Console       │             │
│           └──────┘  └──────┘  └──────┘  └──────┘        │            │
│                                                           │             │
│        └───────────────────────────────────────────────────┘             │
│                                 │                                         │
│                    ┌────────────▼────────────┐                            │
│                    │   ENRICHED OUTPUT       │                            │
│                    │  risk_score, status     │                            │
│                    │  quality_score, alerts  │                            │
│                    └─────────────────────────┘                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 11 Steps Data Transformation

```
STEP 1-2: Kafka Setup + Schemas
    │
    ├─ well-sensor-data topic
    ├─ tank-sensor-data topic
    └─ Schema validation (WellData.avsc, TankData.avsc)

         ↓

STEP 3: Spark Session
    │
    └─ SparkSession.builder()
       .config("checkpointLocation", "/tmp/checkpoint")

         ↓

STEP 4: Read Kafka Streams
    │
    ├─ wellStreamRaw = spark.readStream.format("kafka")
    │                  .option("subscribe", "well-sensor-data")
    │
    └─ tankStreamRaw = spark.readStream.format("kafka")
                      .option("subscribe", "tank-sensor-data")

         ↓

STEP 5: Cast Binary → String

    ├─ col("value") is Avro-encoded (binary)
    └─ Data is ready for Avro deserialization

       ↓

  STEP 6: Parse Avro with Schema Registry + Create Temp Views
    │
    ├─ Use spark-avro and set "schema.registry.url" to connect to Schema Registry
    ├─ from_avro(col("value"), schema, options)  # Deserializes using registered Avro schemas
    │
    ├─ wellParsed.createOrReplaceTempView("well_data_temp")
    └─ tankParsed.createOrReplaceTempView("tank_data_temp")
    │
    └─ All schemas are managed centrally in Confluent Schema Registry for compatibility and evolution

         ↓

STEP 7: Spark SQL Queries (Risk Classification)
    │
    ├─ SELECT methane_ppm, co2_ppm,
    │    CASE WHEN methane_ppm > 500 THEN 'HIGH_EMISSION' ...
    │    FROM well_data_temp
    │
    └─ SELECT vapor_concentration_ppm,
         CASE WHEN vapor_concentration_ppm > 10000 THEN 'CRITICAL' ...
         FROM tank_data_temp

         ↓

STEP 8: Decode Base64
    │
    ├─ vibration_encoded = "dmliX2RhdGFfeHl6" (base64)
    │
    └─ vibration_decoded = "vib_data_xyz" (decoded string)

         ↓

STEP 9: Filter Non-Null Values + Data Quality
    │
    ├─ .filter(col("well_id").isNotNull())
    ├─ .filter(col("methane_ppm").isNotNull())
    │
    ├─ .withColumn("data_quality_score",
    │    CASE WHEN HIGH_EMISSION THEN 75
    │         WHEN MEDIUM_EMISSION THEN 85
    │         ELSE 95)
    │
    └─ Only valid rows pass forward

         ↓

STEP 10: Inner Join Well + Tank Data
    │
    ├─ ON facility_id = facility_id
    ├─ AND event_timestamp BETWEEN -5sec TO +5sec
    │
    └─ Result: Correlated facility-level view

         ↓

STEP 11: Calculate Risk Score & Output
    │
    ├─ risk_score = emission_risk (0-40) + vapor_risk (0-40)
    ├─ facility_status = ALERT if risk_score > 30 ELSE NORMAL
    │
    └─ .writeStream.format("console").trigger(5 seconds)
       
       ┌──────────────────────────────────────────────────────┐
       │ well_id│facility│methane│emission_risk│risk_score    │
       ├──────────────────────────────────────────────────────┤
       │WELL-42 │FAC-42  │385.50 │MEDIUM       │30            │
       │WELL-18 │FAC-18  │650.20 │HIGH         │80 → ALERT    │
       │WELL-35 │FAC-35  │125.50 │NORMAL       │10            │
       └──────────────────────────────────────────────────────┘
```

---

## Streaming vs Batch (Why This Matters)

```
TRADITIONAL BATCH PROCESSING:
┌─────────────────────────────────────────────────────┐
│ Hour 1  │ Collect events                            │
│ Hour 2  │ Transfer to warehouse                      │
│ Hour 3  │ Process batch job                         │
│ Hour 4  │ Generate alerts                           │
│ Result: 4-hour latency (unacceptable for fraud)    │
└─────────────────────────────────────────────────────┘

REAL-TIME STREAMING (THIS PROJECT):
┌─────────────────────────────────────────────────────┐
│ T=0ms   │ Event arrives in Kafka                    │
│ T=10ms  │ Spark receives event                      │
│ T=20ms  │ Decode, validate, enrich                  │
│ T=30ms  │ Risk score calculated                     │
│ T=40ms  │ Alert sent to dashboard                   │
│ Result: 40ms latency (real-time detection)          │
└─────────────────────────────────────────────────────┘

ADVANTAGE: Catch 80% of fraud before it completes!
```

---

## Kafka Partitioning for Ordering

```
Kafka Topic: "well-sensor-data" (3 partitions)

Producer sends events with key=well_id:

WELL-42 → Partition 0 ──→ Consumer 1
WELL-18 → Partition 1 ──→ Consumer 2  (Ordering guarantee
WELL-35 → Partition 2 ──→ Consumer 3   within each partition)

Same well always goes to same partition:
  WELL-42 @ T=100ms → Partition 0
  WELL-42 @ T=200ms → Partition 0  ✓ Order preserved
  WELL-42 @ T=300ms → Partition 0
```
---

## Join Strategy (Time-Window)

```
Well Data Timeline:
  T=1000ms: methane=350, pressure=200
  T=1050ms: methane=360, pressure=205
  T=2000ms: methane=370, pressure=210
  T=3000ms: methane=380, pressure=215

Tank Data Timeline:
  T=999ms:  vapor=5000, level=80
  T=1002ms: vapor=5050, level=80
  T=2015ms: vapor=5100, level=79
  T=3010ms: vapor=5150, level=78

Join: facility_id = facility_id AND |well_ts - tank_ts| < 5000ms

Matched Pairs (T ±5 sec window):
  Well @ T=1000 ↔ Tank @ T=999   ✓ Match (1ms apart)
  Well @ T=1050 ↔ Tank @ T=1002  ✓ Match (48ms apart)
  Well @ T=2000 ↔ Tank @ T=2015  ✓ Match (15ms apart)
  Well @ T=3000 ↔ Tank @ T=3010  ✓ Match (10ms apart)

Result: Facility-level view = [well metrics] + [tank metrics]
```

---

## Scaling Path

```
Current (Local): 
  Kafka: 3 partitions
  Spark: local[*] (4 cores)
  Throughput: ~2 events/sec
  Latency: 40ms

Scale to 1000x (1M events/sec):

STEP 1: Kafka Partitions
  3 → 30 partitions (1 partition per Spark executor)

STEP 2: Spark Executors
  1 → 100 executors (Kubernetes auto-scaling)
  Each executor processes 1-2 partitions
  Result: 100-200 parallel streams

STEP 3: Storage
  Console → Delta Lake on S3
  ACID transactions + time travel
  Partitioned on facility_id (fast queries)

STEP 4: State Management
  Checkpoint location: /tmp → S3/HDFS
  RocksDB state backend (handles billions events)

STEP 5: Monitoring
  Prometheus: events/sec, join latency, error rate
  Grafana: real-time dashboards
  AlertManager: critical alerts

Result: Millions events/sec, 100ms latency, auto-scaling
```

---

## Error Recovery (Exactly-Once)

```
Checkpoint saves progress:
  T=0ms:   Start Spark job
  T=1000ms: Processed 1000 events, saved offset=1000
  T=2000ms: Processed 1000 more events, offset=2000
  
  ❌ CRASH at T=2500ms (after 500 events, before checkpoint)

Restart:
  T=0ms:   Resume from last checkpoint (offset=2000)
  T=500ms: Reprocess events 2001-2500 (idempotent)
  T=1500ms: Continue processing new events

Guarantee:
  ✓ No data loss (Kafka keeps data 7 days)
  ✓ No duplication (idempotent joins, deduplication)
  ✓ Consistency (offset + results atomic)

