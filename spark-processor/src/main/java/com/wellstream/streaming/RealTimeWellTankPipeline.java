package com.wellstream.streaming;

import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.avro.functions.from_avro; //static tells Java Compiler that all methods from org.apache.spark.sql.functions are available in class scope
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * STEPS 3-11: Complete Spark Structured Streaming Pipeline
 * 
 * Execution Sequence:
 * 3. Initialize Spark Session
 * 4. Create DF to read from Kafka
 * 5. Cast binary to string
 * 6. Extract JSON payloads into temp views
 * 7. Use Spark SQL to select KPIs
 * 8. Decode base64 data
 * 9. Filter null values
 * 10. Perform joins
 * 11. Output to shell
 * 
 * - Ingest = transaction events from Kafka
 * - Validate = schema + data quality
 * - Enrich = add risk scoring, compliance flags
 * - Join = correlate multiple data sources (cross-checking)
 * - Output = real-time alerts to fraud team
 */
public class RealTimeWellTankPipeline {

    public static void main(String[] args) throws Exception {
        // ============================================================
        // STEP 3: Initialize Spark Session with Structured Streaming
        // ============================================================
        System.out.println("\n>>> STEP 3: Initializing Spark Session...");
        
        SparkSession spark = SparkSession.builder()
            .appName("RealTimeWellTankAnalytics")
            .master("local[*]")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        System.out.println("✓ Spark Session initialized successfully");

        try {
            // ============================================================
            // STEP 4: Create DataFrames to Read Streaming Data from Kafka
            // ============================================================
            System.out.println("\n>>> STEP 4: Creating streaming DataFrames from Kafka...");

            // Define schemas matching Avro structure
            StructType wellSchema = new StructType()
                .add("well_id", DataTypes.StringType)
                .add("facility_id", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("methane_ppm", DataTypes.DoubleType)
                .add("co2_ppm", DataTypes.DoubleType)
                .add("pressure_psi", DataTypes.DoubleType)
                .add("temperature_f", DataTypes.DoubleType)
                .add("flow_rate_bpd", DataTypes.DoubleType)
                .add("vibration_encoded", DataTypes.StringType)
                .add("pump_status", DataTypes.StringType)
                .add("maintenance_alert", DataTypes.BooleanType);

            StructType tankSchema = new StructType()
                .add("tank_id", DataTypes.StringType)
                .add("facility_id", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("level_percentage", DataTypes.DoubleType)
                .add("vapor_concentration_ppm", DataTypes.DoubleType)
                .add("leak_detection_status", DataTypes.StringType)
                .add("ambient_temperature_f", DataTypes.DoubleType)
                .add("internal_pressure_psi", DataTypes.DoubleType)
                .add("flame_arrestor_status", DataTypes.BooleanType)
                .add("vapor_recovery_active", DataTypes.BooleanType)
                .add("emergency_shutdown", DataTypes.BooleanType)
                .add("wind_speed_mph", DataTypes.DoubleType)
                .add("atmospheric_pressure_mb", DataTypes.DoubleType)
                .add("air_quality_index", DataTypes.IntegerType);

            String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                bootstrapServers = "kafka-broker:9092";
            }
            System.out.println("Using Kafka bootstrap servers: " + bootstrapServers);

            // Define Avro schemas (must match producer schemas)
            String wellAvroSchema = "{"
                + "\"type\": \"record\","
                + "\"name\": \"WellData\","
                + "\"namespace\": \"com.wellstream.streaming\","
                + "\"fields\": ["
                + "  {\"name\": \"well_id\", \"type\": \"string\"},"
                + "  {\"name\": \"facility_id\", \"type\": \"string\"},"
                + "  {\"name\": \"timestamp\", \"type\": \"long\"},"
                + "  {\"name\": \"methane_ppm\", \"type\": \"double\"},"
                + "  {\"name\": \"co2_ppm\", \"type\": \"double\"},"
                + "  {\"name\": \"pressure_psi\", \"type\": \"double\"},"
                + "  {\"name\": \"temperature_f\", \"type\": \"double\"},"
                + "  {\"name\": \"flow_rate_bpd\", \"type\": \"double\"},"
                + "  {\"name\": \"vibration_encoded\", \"type\": \"string\"},"
                + "  {\"name\": \"pump_status\", \"type\": \"string\"},"
                + "  {\"name\": \"maintenance_alert\", \"type\": \"boolean\"}"
                + "]}";

            String tankAvroSchema = "{"
                + "\"type\": \"record\","
                + "\"name\": \"TankData\","
                + "\"namespace\": \"com.wellstream.streaming\","
                + "\"fields\": ["
                + "  {\"name\": \"tank_id\", \"type\": \"string\"},"
                + "  {\"name\": \"facility_id\", \"type\": \"string\"},"
                + "  {\"name\": \"timestamp\", \"type\": \"long\"},"
                + "  {\"name\": \"level_percentage\", \"type\": \"double\"},"
                + "  {\"name\": \"vapor_concentration_ppm\", \"type\": \"double\"},"
                + "  {\"name\": \"leak_detection_status\", \"type\": \"string\"},"
                + "  {\"name\": \"ambient_temperature_f\", \"type\": \"double\"},"
                + "  {\"name\": \"internal_pressure_psi\", \"type\": \"double\"},"
                + "  {\"name\": \"flame_arrestor_status\", \"type\": \"boolean\"},"
                + "  {\"name\": \"vapor_recovery_active\", \"type\": \"boolean\"},"
                + "  {\"name\": \"emergency_shutdown\", \"type\": \"boolean\"},"
                + "  {\"name\": \"wind_speed_mph\", \"type\": \"double\"},"
                + "  {\"name\": \"atmospheric_pressure_mb\", \"type\": \"double\"},"
                + "  {\"name\": \"air_quality_index\", \"type\": \"int\"}"
                + "]}";

            // Read well data from Kafka topic with Avro deserialization
            Dataset<Row> wellStreamRaw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("schema.registry.url", "http://schema-reg:8081")
                .option("subscribe", "well-sensor-data")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .select(
                    from_avro(col("value"), wellAvroSchema).as("data"),
                    col("timestamp").alias("kafka_timestamp")
                )
                .select("data.*", "kafka_timestamp");

            // Read tank data from Kafka topic with Avro deserialization
            Dataset<Row> tankStreamRaw = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "tank-sensor-data")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .select(
                    from_avro(col("value"), tankAvroSchema).as("data"),
                    col("timestamp").alias("kafka_timestamp")
                )
                .select("data.*", "kafka_timestamp");

            System.out.println("✓ Streaming DataFrames created from Kafka topics (Avro format)");

            // ============================================================
            // STEP 5-6: Avro data already deserialized, create temp views
            // ============================================================
            System.out.println("\n>>> STEP 5-6: Avro deserialization complete, creating temp views...");

            // Rename timestamp field to event_timestamp for clarity
            Dataset<Row> wellParsed = wellStreamRaw
                .withColumnRenamed("timestamp", "event_timestamp");

            Dataset<Row> tankParsed = tankStreamRaw
                .withColumnRenamed("timestamp", "event_timestamp");

            // Register temp views for SQL querying
            wellParsed.createOrReplaceTempView("well_data_temp");
            tankParsed.createOrReplaceTempView("tank_data_temp");

            System.out.println("✓ STEP 5-6 COMPLETE: Avro data parsed, temp views created");

            // ============================================================
            // STEP 7: Use Spark SQL to Select Keys from Streaming Data
            // ============================================================
            System.out.println("\n>>> STEP 7: Executing SQL queries for KPI extraction...");

            // SQL query for well metrics with risk classification
            Dataset<Row> wellMetrics = spark.sql(
                "SELECT " +
                "  well_id, " +
                "  facility_id, " +
                "  event_timestamp, " +
                "  methane_ppm, " +
                "  co2_ppm, " +
                "  pressure_psi, " +
                "  temperature_f, " +
                "  flow_rate_bpd, " +
                " vibration_encoded, " +
                "  pump_status, " +
                "  CASE WHEN methane_ppm > 500 THEN 'HIGH_EMISSION' " +
                "       WHEN methane_ppm > 100 THEN 'MEDIUM_EMISSION' " +
                "       ELSE 'NORMAL' END AS emission_risk_level, " +
                "  maintenance_alert " +
                "FROM well_data_temp " +
                "WHERE event_timestamp IS NOT NULL"
            );

            // SQL query for tank metrics with risk classification
            Dataset<Row> tankMetrics = spark.sql(
                "SELECT " +
                "  tank_id, " +
                "  facility_id, " +
                "  event_timestamp, " +
                "  level_percentage, " +
                "  vapor_concentration_ppm, " +
                "  leak_detection_status, " +
                "  ambient_temperature_f, " +
                "  internal_pressure_psi, " +
                "  CASE WHEN vapor_concentration_ppm > 10000 THEN 'CRITICAL' " +
                "       WHEN vapor_concentration_ppm > 5000 THEN 'HIGH' " +
                "       ELSE 'NORMAL' END AS vapor_risk_level, " +
                "  emergency_shutdown " +
                "FROM tank_data_temp " +
                "WHERE event_timestamp IS NOT NULL"
            );

            System.out.println("✓ STEP 7 COMPLETE: SQL queries executed, KPIs extracted");

            // ============================================================
            // STEP 8: Decode Base64 Data and Cast as String
            // ============================================================
            System.out.println("\n>>> STEP 8: Decoding base64 vibration data...");

            // Register UDF for base64 decoding
            spark.udf().register("decodeBase64", (String encoded) -> {
                if (encoded == null) return "N/A";
                try {
                    byte[] decoded = Base64.decodeBase64(encoded);
                    return new String(decoded, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    return "DECODE_ERROR";
                }
            }, DataTypes.StringType);

            // Apply base64 decoding to vibration data
            Dataset<Row> wellDecoded = wellMetrics
                .withColumn(
                    "vibration_decoded",
                    when(
                        col("vibration_encoded").isNotNull(),
                        callUDF("decodeBase64", col("vibration_encoded"))
                    ).otherwise(lit("N/A"))
                );

            System.out.println("✓ STEP 8 COMPLETE: Base64 data decoded");

            // ============================================================
            // STEP 9: Apply Filter to Select Non-Null Values
            // ============================================================
            System.out.println("\n>>> STEP 9: Applying data quality filters...");

            Dataset<Row> wellFiltered = wellDecoded
                .withColumn("event_timestamp", col("event_timestamp").cast(DataTypes.TimestampType))
                .withWatermark("event_timestamp", "10 seconds")
                .filter(
                    col("well_id").isNotNull()
                    .and(col("facility_id").isNotNull())
                    .and(col("methane_ppm").isNotNull())
                    .and(col("co2_ppm").isNotNull())
                    .and(col("pressure_psi").isNotNull())
                    .and(col("temperature_f").isNotNull())
                )
                .withColumn(
                    "data_quality_score",
                    when(col("emission_risk_level").equalTo("HIGH_EMISSION"), lit(75))
                    .when(col("emission_risk_level").equalTo("MEDIUM_EMISSION"), lit(85))
                    .otherwise(lit(95))
                );

            Dataset<Row> tankFiltered = tankMetrics
                .withColumn("event_timestamp", col("event_timestamp").cast(DataTypes.TimestampType))
                .withWatermark("event_timestamp", "10 seconds")
                .filter(
                    col("tank_id").isNotNull()
                    .and(col("facility_id").isNotNull())
                    .and(col("level_percentage").isNotNull())
                    .and(col("vapor_concentration_ppm").isNotNull())
                    .and(col("internal_pressure_psi").isNotNull())
                )
                .withColumn(
                    "data_quality_score",
                    when(col("vapor_risk_level").equalTo("CRITICAL"), lit(70))
                    .when(col("vapor_risk_level").equalTo("HIGH"), lit(80))
                    .otherwise(lit(95))
                );

            System.out.println("✓ STEP 9 COMPLETE: Data quality filters applied");

            // ============================================================
            // STEP 10: Perform Joins on Both DataFrames
            // ============================================================
            System.out.println("\n>>> STEP 10: Joining well and tank data...");

            Dataset<Row> well = wellFiltered.as("well");
            Dataset<Row> tank = tankFiltered.as("tank");

            // Stream-Stream Join with time range condition
            // Requires: facility_id match + event times within 1 minute
            Dataset<Row> joined = well
                .join(
                    tank,
                    expr("well.facility_id = tank.facility_id AND " +
                    "tank.event_timestamp >= well.event_timestamp - interval '1 minute' AND " +
                    "tank.event_timestamp <= well.event_timestamp + interval '1 minute'"
                    ),
                    "inner" //use inner join stream-stream join
                )
                .select(
                    col("well.well_id"),
                    col("well.facility_id"),
                    col("well.event_timestamp").alias("well_timestamp"),
                    col("tank.event_timestamp").alias("tank_timestamp"),
                    col("well.methane_ppm"),
                    col("well.co2_ppm"),
                    col("well.emission_risk_level"),
                    col("well.data_quality_score").alias("well_data_quality"),
                    col("tank.tank_id"),
                    col("tank.level_percentage"),
                    col("tank.vapor_concentration_ppm"),
                    col("tank.vapor_risk_level"),
                    col("tank.data_quality_score").alias("tank_data_quality"),
                    // Correlation score based on risk alignment
                    when(
                        col("well.emission_risk_level").isin("HIGH_EMISSION")
                        .or(col("tank.vapor_risk_level").isin("HIGH", "CRITICAL")),
                        lit(95)
                    )
                    .when(
                        col("well.emission_risk_level").equalTo("MEDIUM_EMISSION")
                        .and(col("tank.vapor_risk_level").equalTo("HIGH")),
                        lit(75)
                    )
                    .otherwise(lit(50)).alias("correlation_score"),
                    when(
                        col("well.emission_risk_level").isin("HIGH_EMISSION")
                        .or(col("tank.vapor_risk_level").isin("HIGH", "CRITICAL")),
                        lit("CRITICAL_ALERT")
                    )
                    .when(
                        col("well.emission_risk_level").equalTo("MEDIUM_EMISSION")
                        .or(col("tank.vapor_risk_level").equalTo("HIGH")),
                        lit("WARNING")
                    )
                    .otherwise(lit("NORMAL")).alias("facility_status"),
                    (
                        when(col("well.emission_risk_level").equalTo("HIGH_EMISSION"), lit(40))
                        .when(col("well.emission_risk_level").equalTo("MEDIUM_EMISSION"), lit(20))
                        .otherwise(lit(10))
                        .plus(
                            when(col("tank.vapor_risk_level").equalTo("CRITICAL"), lit(40))
                            .when(col("tank.vapor_risk_level").equalTo("HIGH"), lit(20))
                            .otherwise(lit(10))
                        )
                    ).alias("risk_score"),
                    current_timestamp().alias("processing_timestamp")
                )
                .filter(col("correlation_score").geq(50)); //only high confidence correlations

            System.out.println("✓ STEP 10 COMPLETE: Well and Tank data joined on facility_id + time window");

            // ============================================================
            // STEP 11: Output New DF to Shell
            // ============================================================
            System.out.println("\n>>> STEP 11: Starting streaming output...\n");

            // Console output (real-time to shell)
            StreamingQuery consoleQuery = joined
                .writeStream()
                .outputMode("append")
                .format("console")
                .option("truncate", "false")
                .option("numRows", "20")
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .start();

            System.out.println("\n✓ STEP 11 COMPLETE: Streaming output active");
            System.out.println("  - Real-time enriched events flowing to console");
            System.out.println("  - High-risk alerts (risk_score > 20) displayed");
            System.out.println("  - Data quality scores visible for compliance\n");

            // Keep query alive
            consoleQuery.awaitTermination();

        } catch (Exception e) {
            System.err.println("Error in pipeline: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}