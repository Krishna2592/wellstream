# Schema Registry + KSQL Integration - Remaining Steps

## ✅ Completed Changes

1. **Docker Compose** - Schema Registry and KSQL servers added
2. **Parent POM** - Confluent repository and dependencies added  
3. **Producer POM** - Avro serializer dependencies added
4. **Spark Processor POM** - spark-avro dependency added
5. **Producer Code** - Partially converted to Avro (needs rebuild)
6. **Spark Code** - Partially converted to Avro (needs rebuild)

## ⚠️ Build Required

The Maven dependencies were added but need to be downloaded:

```bash
# Clean and rebuild with new dependencies
mvn clean install -DskipTests

# This will download:
# - io.confluent:kafka-avro-serializer
# - io.confluent:kafka-schema-registry-client  
# - org.apache.spark:spark-avro_2.12
# - org.apache.avro:avro-*
```

## 🔧 Manual Fixes Needed

### 1. FakeDataProducer.java - Remove Unused Imports

The file still has these unused imports that should be removed:
```java
// REMOVE THESE:
import redis.clients.jedis.Jedis;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;  // Keep GenericRecord only
```

Keep only:
```java
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
```

### 2. Remove Redis Code (producer)

Since we're using Avro now, remove the Redis caching section (lines ~31-40):

```java
// DELETE THIS BLOCK:
ObjectMapper mapper = new ObjectMapper();

String redisHost = System.getenv("REDIS_HOST");
if (redisHost == null) {
    redisHost = "localhost";
}
Jedis jedis = new Jedis(redisHost, 6379);
System.out.println("✓ Connected to Redis: " + redisHost);
```

### 3. docker-compose.yml - Add Schema Registry URL to Services

Update environment variables for producer and spark-processor:

```yaml
producer:
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    SCHEMA_REGISTRY_URL: http://schema-registry:8081  # ADD THIS
    REDIS_HOST: redis

spark-processor:
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    SCHEMA_REGISTRY_URL: http://schema-registry:8081  # ADD THIS
```

### 4. Update Spark Processor Import

The import needs to use the proper function:

```java
// Current (wrong):
import static org.apache.spark.sql.avro.functions.from_avro;

// Should be:
import org.apache.spark.sql.avro.functions;
// Then use: functions.from_avro(...)
```

**OR** remove the static import and just call it directly:

```java
.select(
    org.apache.spark.sql.avro.functions.from_avro(col("value"), wellAvroSchema).as("data"),
    col("timestamp").alias("kafka_timestamp")
)
```

### 5. Remove Old StructType Schemas (Spark)

These are now unused since we're using Avro schemas:

```java
// DELETE LINES ~67-96 (wellSchema and tankSchema StructType definitions)
StructType wellSchema = new StructType()
    .add("well_id", DataTypes.StringType)
    // ... DELETE ALL THIS ...

StructType tankSchema = new StructType()
    .add("tank_id", DataTypes.StringType)
    // ... DELETE ALL THIS ...
```

## 🚀 Testing After Fixes

### 1. Rebuild Everything

```bash
mvn clean package -DskipTests
```

### 2. Start Services

```bash
# Start base stack + schema registry
docker compose up -d

# Check schema registry health
curl http://localhost:8081/subjects

# Check KSQL server
curl http://localhost:8088/info
```

### 3. Verify Avro Schemas Auto-Registered

After producer starts sending data:

```bash
# Should see schemas registered
curl http://localhost:8081/subjects

# Should return:
# ["well-sensor-data-value", "tank-sensor-data-value"]

# Get schema details
curl http://localhost:8081/subjects/well-sensor-data-value/versions/latest
```

### 4. Test KSQL Integration

```bash
# Access KSQL CLI
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

# In KSQL shell:
SHOW TOPICS;
PRINT 'well-sensor-data' FROM BEGINNING LIMIT 5;
```

## 📁 New Files to Create (Optional)

### avro-schemas/well-data.avsc

```json
{
  "type": "record",
  "name": "WellData",
  "namespace": "com.wellstream.streaming",
  "fields": [
    {"name": "well_id", "type": "string"},
    {"name": "facility_id", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "methane_ppm", "type": "double"},
    {"name": "co2_ppm", "type": "double"},
    {"name": "pressure_psi", "type": "double"},
    {"name": "temperature_f", "type": "double"},
    {"name": "flow_rate_bpd", "type": "double"},
    {"name": "vibration_encoded", "type": "string"},
    {"name": "pump_status", "type": "string"},
    {"name": "maintenance_alert", "type": "boolean"}
  ]
}
```

### avro-schemas/tank-data.avsc

```json
{
  "type": "record",
  "name": "TankData",
  "namespace": "com.wellstream.streaming",
  "fields": [
    {"name": "tank_id", "type": "string"},
    {"name": "facility_id", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "level_percentage", "type": "double"},
    {"name": "vapor_concentration_ppm", "type": "double"},
    {"name": "leak_detection_status", "type": "string"},
    {"name": "ambient_temperature_f", "type": "double"},
    {"name": "internal_pressure_psi", "type": "double"},
    {"name": "flame_arrestor_status", "type": "boolean"},
    {"name": "vapor_recovery_active", "type": "boolean"},
    {"name": "emergency_shutdown", "type": "boolean"},
    {"name": "wind_speed_mph", "type": "double"},
    {"name": "atmospheric_pressure_mb", "type": "double"},
    {"name": "air_quality_index", "type": "int"}
  ]
}
```

## 🎯 Summary

**Core issue**: Maven needs to download the new Confluent dependencies before the code will compile.

**Quick fix sequence**:
1. Run `mvn clean install` to download dependencies
2. Remove unused imports (Redis, ObjectMapper, etc.)
3. Remove unused StructType schemas from Spark code
4. Fix the `from_avro` import syntax
5. Add `SCHEMA_REGISTRY_URL` env var to docker-compose
6. Rebuild and test

The infrastructure (Schema Registry, KSQL) is already configured in docker-compose.yml and will start correctly.
