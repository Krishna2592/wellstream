# WellStream Architecture

This document describes the end-to-end architecture of the WellStream real-time streaming pipeline.

## System Overview

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
│    │ Init   │→ │ Read  │→ │ Cast  │→ │ JSON  │→ │ SQL   │   │            │
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
    │
    ├─ col("value").cast(DataTypes.StringType)
    │
    └─ Now: binary data → readable JSON strings

         ↓

STEP 6: Parse JSON + Create Temp Views
    │
    ├─ from_json(col("json_value"), wellSchema)
    ├─ from_json(col("json_value"), tankSchema)
    │
    ├─ wellParsed.createOrReplaceTempView("well_data_temp")
    └─ tankParsed.createOrReplaceTempView("tank_data_temp")

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

## Streaming vs Batch Processing

### Traditional Batch Processing

```
┌─────────────────────────────────────────────────────┐
│ Hour 1  │ Collect events                            │
│ Hour 2  │ Transfer to warehouse                      │
│ Hour 3  │ Process batch job                         │
│ Hour 4  │ Generate alerts                           │
│ Result: 4-hour latency                              │
└─────────────────────────────────────────────────────┘
```

### Real-Time Streaming (WellStream)

```
┌─────────────────────────────────────────────────────┐
│ T=0ms   │ Event arrives in Kafka                    │
│ T=10ms  │ Spark receives event                      │
│ T=20ms  │ Decode, validate, enrich                  │
│ T=30ms  │ Risk score calculated                     │
│ T=40ms  │ Alert sent to dashboard                   │
│ Result: 40ms latency (real-time detection)          │
└─────────────────────────────────────────────────────┘
```

**Advantage**: Real-time detection enables immediate response to critical events (methane leaks, tank pressure spikes).

## Kafka Partitioning Strategy

Kafka ensures ordering within each partition by routing events with the same key to the same partition:

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

**Why this matters**: Ensures sequential processing of sensor readings from the same well, critical for time-series analysis.

## Time-Window Join Strategy

Correlates well and tank data from the same facility within a 5-second window:

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

**Implementation**: Spark's `join()` with watermark-based state management handles late-arriving data.

## Scaling Path

### Current Setup (Local)

```
Kafka: 3 partitions
Spark: local[*] (4 cores)
Throughput: ~2 events/sec
Latency: 40ms
```

### Production Scale (1M events/sec)

```
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

## Fault Tolerance & Exactly-Once Semantics

Checkpointing ensures exactly-once processing:

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
```
## Schema Evolution with Confluent Schema Registry

- Backward compatibility: old consumers read new data
- Forward compatibility: new consumers read old data
- Full compatibility: both directions
- Centralized schema versioning

## Data Flow Summary

1. **Ingestion**: IoT sensors → Kafka topics (well-sensor-data, tank-sensor-data)
2. **Buffering**: Kafka retains events with configurable retention (default 7 days)
3. **Processing**: Spark Structured Streaming consumes from Kafka
4. **Transformation**: Binary → JSON → SQL → Risk Scoring
5. **Enrichment**: Join well + tank data, decode base64, add quality scores
6. **Output**: Console sink (dev) or Delta Lake (prod)
7. **Monitoring**: Prometheus scrapes metrics from Spark UI
8. **Visualization**: Grafana dashboards show real-time pipeline health

## Technology Decisions

| Decision | Rationale |
|----------|-----------|
| **Kafka over RabbitMQ** | Kafka provides durable storage, ordering guarantees, and horizontal scalability needed for streaming workloads |
| **Spark over Flink** | Spark SQL is more accessible to data analysts, has better Delta Lake integration, and offers MLlib for ML pipelines |
| **Redis for caching** | In-memory cache for frequently accessed wells (e.g., those divisible by 5), reduces Kafka read load |
| **Structured Streaming over DStreams** | Structured Streaming uses DataFrame/Dataset API, provides automatic checkpointing, and integrates with Catalyst optimizer |
| **Docker Compose for dev** | Reproducible local environment without cloud dependencies |
| **Kubernetes for prod** | StatefulSets for Kafka/Zookeeper, horizontal pod autoscaling, and production-grade orchestration |

## Future Enhancements

- **Delta Lake Sink**: Replace console output with Delta tables for ACID transactions and time-travel queries
- **ML Integration**: Deploy predictive maintenance models using MLflow
- **Multi-Region Deployment**: Active-active Kafka replication across Azure regions

