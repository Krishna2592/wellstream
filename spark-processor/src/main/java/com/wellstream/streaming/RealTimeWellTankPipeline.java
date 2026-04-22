package com.wellstream.streaming;

import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.avro.functions.from_avro;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.least;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.when;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import redis.clients.jedis.Jedis;

/**
 * STEPS 3-13: Spark Structured Streaming Pipeline
 * Real-Time Well & Tank Safety Monitoring
 *
 * Execution Sequence:
 *  3.  Initialize Spark Session
 *  4.  Create streaming DataFrames from Kafka (well-sensor-data, tank-sensor-data)
 *  5.  Avro deserialization via Confluent Schema Registry schemas
 *  6.  Register temp views for Spark SQL querying
 *  7.  SQL-based emission and vapor risk classification
 *  8.  Decode base64 vibration telemetry via UDF
 *  9.  Filter nulls + compute data quality scores
 *  10. Stream-stream join on facility_id + 1-minute time window
 *  11. Assemble multi-sensor feature vectors (Spark MLlib VectorAssembler)
 *  12. Batch Z-score anomaly detection on methane concentration
 *  13. Write facility health scores to Redis (SCADA hot-path cache)
 *
 * Regulatory context:
 *  - Methane thresholds per EPA 40 CFR Part 60 Subpart OOOOa (Method 21: >500 ppm = leak)
 *  - Vapor concentration thresholds per OSHA 29 CFR 1910.119 (PSM Standard)
 *  - Facility status drives real-time alerts to HSE operations center
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
            // STEP 4: Create DataFrames to Read Streaming Sensor Telemetry from Kafka
            // ============================================================
            System.out.println("\n>>> STEP 4: Creating streaming DataFrames from Kafka...");

            // Well sensor telemetry schema: subsurface production metrics
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

            // Storage tank telemetry schema: surface facility metrics
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

            // Avro schemas for deserialization — must match schemas registered in Confluent Schema Registry
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

            // Read well telemetry from Kafka with Avro deserialization
            Dataset<Row> wellStreamRaw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "well-sensor-data")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .select(
                    from_avro(col("value"), wellAvroSchema).as("data"),
                    col("timestamp").alias("kafka_timestamp")
                )
                .select("data.*", "kafka_timestamp");

            // Read tank telemetry from Kafka with Avro deserialization
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
            // STEP 5-6: Avro Deserialization Complete — Create Temp Views
            // ============================================================
            System.out.println("\n>>> STEP 5-6: Avro deserialization complete, creating temp views...");

            Dataset<Row> wellParsed = wellStreamRaw
                .withColumnRenamed("timestamp", "event_timestamp");

            Dataset<Row> tankParsed = tankStreamRaw
                .withColumnRenamed("timestamp", "event_timestamp");

            wellParsed.createOrReplaceTempView("well_data_temp");
            tankParsed.createOrReplaceTempView("tank_data_temp");

            System.out.println("✓ STEP 5-6 COMPLETE: Avro data parsed, temp views registered");

            // ============================================================
            // STEP 7: Spark SQL — Emission and Vapor Risk Classification
            //
            // Well thresholds:  EPA 40 CFR Part 60 Subpart OOOOa (Method 21)
            //   >500 ppm CH4 = equipment leak → triggers LDAR inspection
            // Tank thresholds: OSHA 29 CFR 1910.119 (Process Safety Management)
            //   >10,000 ppm VOC = CRITICAL; >5,000 ppm = elevated LDAR trigger
            // ============================================================
            System.out.println("\n>>> STEP 7: Executing SQL queries for emission risk classification...");

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
                "  vibration_encoded, " +
                "  pump_status, " +
                "  CASE WHEN methane_ppm > 500 THEN 'HIGH_EMISSION' " +
                "       WHEN methane_ppm > 100 THEN 'MEDIUM_EMISSION' " +
                "       ELSE 'NORMAL' END AS emission_risk_level, " +
                "  maintenance_alert " +
                "FROM well_data_temp " +
                "WHERE event_timestamp IS NOT NULL"
            );

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

            System.out.println("✓ STEP 7 COMPLETE: Emission and vapor risk levels classified");

            // ============================================================
            // STEP 8: Decode Base64 Vibration Telemetry
            // Pump and compressor vibration data is base64-encoded at source
            // ============================================================
            System.out.println("\n>>> STEP 8: Decoding base64 vibration telemetry...");

            spark.udf().register("decodeBase64", (String encoded) -> {
                if (encoded == null) return "N/A";
                try {
                    byte[] decoded = Base64.decodeBase64(encoded);
                    return new String(decoded, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    return "DECODE_ERROR";
                }
            }, DataTypes.StringType);

            Dataset<Row> wellDecoded = wellMetrics
                .withColumn(
                    "vibration_decoded",
                    when(
                        col("vibration_encoded").isNotNull(),
                        callUDF("decodeBase64", col("vibration_encoded"))
                    ).otherwise(lit("N/A"))
                );

            System.out.println("✓ STEP 8 COMPLETE: Vibration telemetry decoded");

            // ============================================================
            // STEP 9: Data Quality Filtering + Quality Scoring
            // Incomplete sensor readings are dropped before join
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
            // STEP 10: Stream-Stream Join — Well + Tank on facility_id + time window
            // Correlates subsurface and surface readings for facility-level view
            // ============================================================
            System.out.println("\n>>> STEP 10: Joining well and tank telemetry by facility...");

            Dataset<Row> well = wellFiltered.as("well");
            Dataset<Row> tank = tankFiltered.as("tank");

            Dataset<Row> joined = well
                .join(
                    tank,
                    expr("well.facility_id = tank.facility_id AND " +
                        "tank.event_timestamp >= well.event_timestamp - interval '1 minute' AND " +
                        "tank.event_timestamp <= well.event_timestamp + interval '1 minute'"
                    ),
                    "inner"
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
                .filter(col("correlation_score").geq(50));

            System.out.println("✓ STEP 10 COMPLETE: Well and tank telemetry joined on facility_id + time window");

            // ============================================================
            // STEP 11-13: foreachBatch — Anomaly Detection, Redis Cache, Output
            //
            // foreachBatch processes each micro-batch as a static DataFrame,
            // unlocking full MLlib and external system integration:
            //
            //   STEP 11: VectorAssembler assembles multi-sensor feature vectors
            //            for ML model readiness (isolation forest, k-means clustering)
            //   STEP 12: Z-score anomaly detection on methane_ppm per micro-batch
            //            + predictive maintenance scoring from combined risk signals
            //   STEP 13: Write facility health payloads to Redis (5-min TTL)
            //            Key pattern: facility:{id}:health — consumed by SCADA dashboards
            // ============================================================
            System.out.println("\n>>> STEP 11-13: Starting foreachBatch pipeline (anomaly detection + Redis)...\n");

            // STEP 11: Assemble multi-sensor feature vector for ML model integration
            // Features: methane concentration, CO2, vapor concentration, tank fill level
            VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"methane_ppm", "co2_ppm", "vapor_concentration_ppm", "level_percentage"})
                .setOutputCol("sensor_features")
                .setHandleInvalid("skip");

            StreamingQuery consoleQuery = joined
                .writeStream()
                .outputMode("append")
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) -> {
                    if (batchDF.isEmpty()) return;

                    // STEP 12: Batch-relative Z-score anomaly detection
                    // Flags readings statistically deviant from the current micro-batch mean.
                    // Production note: replace batch stats with rolling baseline from Delta Lake
                    // history table for more robust population-level anomaly scoring.
                    Row batchStats = batchDF.agg(
                        avg("methane_ppm").as("mean_methane"),
                        stddev("methane_ppm").as("std_methane")
                    ).first();

                    double meanMethane = batchStats.isNullAt(0) ? 200.0 : batchStats.getDouble(0);
                    double stdMethane  = (batchStats.isNullAt(1) || batchStats.getDouble(1) == 0.0)
                                        ? 1.0 : batchStats.getDouble(1);

                    Dataset<Row> withAnomalyScore = batchDF
                        .withColumn("methane_zscore",
                            col("methane_ppm").minus(lit(meanMethane)).divide(lit(stdMethane)))
                        // Predictive maintenance score: weighted combination of emission, vapor, and level signals
                        // Score ranges 0-100; >70 = maintenance required, 40-70 = elevated wear
                        .withColumn("predictive_maintenance_score",
                            when(col("emission_risk_level").equalTo("HIGH_EMISSION"), lit(40))
                            .when(col("emission_risk_level").equalTo("MEDIUM_EMISSION"), lit(20))
                            .otherwise(lit(0))
                            .plus(
                                when(col("vapor_risk_level").equalTo("CRITICAL"), lit(40))
                                .when(col("vapor_risk_level").equalTo("HIGH"), lit(20))
                                .otherwise(lit(0))
                            )
                            .plus(
                                // Tank fill extremes (<10% or >90%) stress seals and pressure relief valves
                                when(col("level_percentage").gt(90).or(col("level_percentage").lt(10)), lit(20))
                                .otherwise(lit(0))
                            ))
                        .withColumn("is_anomaly",
                            abs(col("methane_zscore").cast(DataTypes.DoubleType)).gt(2.0)
                            .or(col("risk_score").gt(60)));

                    // STEP 11 (applied per batch): assemble feature vector for ML readiness
                    Dataset<Row> withFeatures = assembler.transform(withAnomalyScore);

                    // STEP 13: Write facility health payloads to Redis
                    // Only anomalous facilities are cached to keep Redis footprint small
                    String redisHost = System.getenv("REDIS_HOST");
                    if (redisHost == null || redisHost.isBlank()) redisHost = "redis";

                    try (Jedis jedis = new Jedis(redisHost, 6379)) {
                        withFeatures
                            .filter(col("is_anomaly").equalTo(true))
                            .select("facility_id", "risk_score", "facility_status",
                                    "methane_zscore", "predictive_maintenance_score")
                            .collectAsList()
                            .forEach(row -> {
                                String facilityId = row.getAs("facility_id");
                                int    riskScore   = ((Number) row.getAs("risk_score")).intValue();
                                int    mxScore     = ((Number) row.getAs("predictive_maintenance_score")).intValue();
                                String status      = row.getAs("facility_status");
                                double zscore      = ((Number) row.getAs("methane_zscore")).doubleValue();
                                String payload = String.format(
                                    "{\"risk_score\":%d,\"status\":\"%s\",\"methane_zscore\":%.2f,"
                                    + "\"predictive_maintenance_score\":%d,\"anomaly\":true,\"batch_id\":%d}",
                                    riskScore, status, zscore, mxScore, batchId
                                );
                                // TTL: 300 seconds (5 minutes) — refreshed each micro-batch
                                jedis.setex("facility:" + facilityId + ":health", 300, payload);
                                System.out.printf("  [ANOMALY] %s → risk=%d  z=%.2f  mx_score=%d  → cached in Redis%n",
                                    facilityId, riskScore, zscore, mxScore);
                            });
                    } catch (Exception e) {
                        System.err.println("Redis write skipped (will retry next batch): " + e.getMessage());
                    }

                    // Emit enriched batch to console (drop raw feature vector for readability)
                    withFeatures.drop("sensor_features").show(20, false);
                })
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .start();

            System.out.println("✓ STEP 11-13 COMPLETE: Facility safety pipeline active");
            System.out.println("  - Multi-sensor feature vectors assembled (Spark MLlib)");
            System.out.println("  - Z-score anomaly detection running per micro-batch");
            System.out.println("  - Predictive maintenance scores computed per facility");
            System.out.println("  - Anomalous facility health cached in Redis (5-min TTL)");
            System.out.println("  - Enriched events streaming to console every 5 seconds\n");

            // ============================================================
            // PUMP HEALTH MONITORING — Concurrent streaming query on well telemetry
            //
            // Runs independently of the facility safety query above.
            // Consumes the same well sensor stream for pump-specific diagnostics:
            //   - Physics-informed feature engineering (BEP deviation analysis)
            //   - Pump Health Index (PHI 0-100) weighted across flow/pressure/thermal/vibration
            //   - 6-mode failure diagnosis: scale, impeller wear, bearing, seal, GVF, cavitation
            //   - API 610 L1-L4 tiered alarm escalation with recommended actions
            //   - Pump alarm payloads written to Redis: pump:{well_id}:alarm (10-min TTL)
            // ============================================================
            System.out.println("\n>>> PUMP HEALTH: Starting pump failure prediction pipeline...");

            StreamingQuery pumpHealthQuery = wellFiltered
                .writeStream()
                .outputMode("append")
                .option("checkpointLocation", "/tmp/checkpoint-pump")
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) -> {

                    // Only evaluate active pumps — standby and inactive units are not at risk
                    Dataset<Row> activePumps = batchDF.filter(
                        col("pump_status").equalTo("active")
                        .and(col("flow_rate_bpd").isNotNull())
                        .and(col("pressure_psi").isNotNull())
                        .and(col("temperature_f").isNotNull())
                    );
                    if (activePumps.isEmpty()) return;

                    // Run full pump health pipeline: features → PHI → diagnosis → alarms
                    Dataset<Row> pumpReport = PumpHealthMonitor.evaluate(activePumps);

                    // Collect all active pump rows once — used for both history recording and alarms
                    List<Row> allPumpRows = pumpReport
                        .select("well_id", "pump_health_index", "pump_alarm_level", "failure_mode",
                                "rul_estimate", "recommended_action",
                                "flow_deviation_pct", "pressure_deviation_pct",
                                "temp_rise_f", "vibration_severity", "facility_id")
                        .collectAsList();

                    String redisHost = System.getenv("REDIS_HOST");
                    if (redisHost == null || redisHost.isBlank()) redisHost = "redis";

                    // ML RUL predictions for bearing failure wells (populated inside Redis block)
                    Map<String, String> mlRulByWell = new HashMap<>();

                    try (Jedis jedis = new Jedis(redisHost, 6379)) {

                        // ---------------------------------------------------------
                        // STAGE 1: Record PHI history for all active pumps.
                        // This runs every batch regardless of alarm state so that
                        // history accumulates before a bearing fault is diagnosed.
                        // ---------------------------------------------------------
                        long batchTs = System.currentTimeMillis();
                        for (Row row : allPumpRows) {
                            String wellId = row.getAs("well_id");
                            double phi    = ((Number) row.getAs("pump_health_index")).doubleValue();
                            BearingRulPredictor.recordPhiHistory(wellId, phi, batchTs, jedis);
                        }

                        // ---------------------------------------------------------
                        // STAGE 2: ML-based RUL prediction (BEARING_FAILURE_RISK only).
                        //
                        // LinearRegression is fit on the PHI time-series stored in Redis.
                        // Extrapolates when PHI will reach the critical threshold (25.0).
                        // For all other failure modes, the qualitative RUL from
                        // PumpHealthMonitor is used — it's the right tool for those modes.
                        //
                        // Production note: when Delta Lake is the history store, replace
                        // BearingRulPredictor.predictRul() with a batch read + the same
                        // LinearRegression logic, then register the model in MLflow.
                        // ---------------------------------------------------------
                        SparkSession batchSpark = activePumps.sparkSession();
                        for (Row row : allPumpRows) {
                            if ("BEARING_FAILURE_RISK".equals(row.getAs("failure_mode"))) {
                                String wellId  = row.getAs("well_id");
                                String mlRul   = BearingRulPredictor.predictRul(wellId, jedis, batchSpark);
                                mlRulByWell.put(wellId, mlRul);
                            }
                        }

                        // Print ML RUL section if any bearing wells are active
                        if (!mlRulByWell.isEmpty()) {
                            System.out.printf("%n  ┌─ ML RUL — Bearing Failure — LinearRegression on PHI history (batch %d) ─┐%n", batchId);
                            mlRulByWell.forEach((wid, mlRul) ->
                                System.out.printf("  │  %-10s  →  %s%n", wid, mlRul));
                            System.out.println("  └──────────────────────────────────────────────────────────────────────────┘");
                        }

                        // ---------------------------------------------------------
                        // STAGE 3: Write L2+ alarms to Redis.
                        // For bearing failure wells, rul in the payload is the ML prediction.
                        // For all other failure modes, rul is the qualitative estimate.
                        // Key: pump:{well_id}:alarm  TTL: 600s (10 min)
                        // ---------------------------------------------------------
                        for (Row row : allPumpRows) {
                            String alarmLevel = row.getAs("pump_alarm_level");
                            if (!alarmLevel.equals("L4_SHUTDOWN")
                                    && !alarmLevel.equals("L3_CRITICAL")
                                    && !alarmLevel.equals("L2_WARNING")) continue;

                            String wellId     = row.getAs("well_id");
                            String failureMode= row.getAs("failure_mode");
                            double phi        = ((Number) row.getAs("pump_health_index")).doubleValue();
                            double flowDev    = ((Number) row.getAs("flow_deviation_pct")).doubleValue();
                            double presDev    = ((Number) row.getAs("pressure_deviation_pct")).doubleValue();
                            double tempRise   = ((Number) row.getAs("temp_rise_f")).doubleValue();
                            String action     = row.getAs("recommended_action");

                            // Use ML-predicted RUL for bearing failures; qualitative for everything else
                            String rul = mlRulByWell.getOrDefault(wellId, row.getAs("rul_estimate"));

                            String payload = String.format(
                                "{\"alarm\":\"%s\",\"failure_mode\":\"%s\",\"phi\":%.1f,"
                                + "\"rul\":\"%s\",\"rul_method\":\"%s\","
                                + "\"flow_dev\":%.1f,\"pres_dev\":%.1f,"
                                + "\"temp_rise\":%.1f,\"batch\":%d}",
                                alarmLevel, failureMode, phi,
                                rul, mlRulByWell.containsKey(wellId) ? "linear_regression" : "rule_based",
                                flowDev, presDev, tempRise, batchId
                            );
                            jedis.setex("pump:" + wellId + ":alarm", 600, payload);

                            System.out.printf(
                                "  [PUMP %-12s] %-8s  PHI=%5.1f  mode=%-34s%n"
                                + "                   RUL: %s%n"
                                + "                   action: %s%n",
                                alarmLevel, wellId, phi, failureMode, rul, action
                            );
                        }

                    } catch (Exception e) {
                        System.err.println("Redis pump block skipped: " + e.getMessage());
                    }

                    // Print full pump health summary (all active pumps, not just alarms)
                    System.out.printf("%n=== PUMP HEALTH SUMMARY — Batch %d ===%n", batchId);
                    pumpReport
                        .select("well_id", "pump_status", "pump_health_index", "pump_alarm_level",
                                "failure_mode", "rul_estimate",
                                "flow_deviation_pct", "pressure_deviation_pct",
                                "temp_rise_f", "vibration_severity",
                                "hydraulic_efficiency_pct")
                        .show(20, false);
                })
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
                .start();

            System.out.println("✓ Pump health monitoring pipeline active (10-second trigger)");
            System.out.println("  - Physics-based feature engineering (BEP deviation analysis)");
            System.out.println("  - Pump Health Index: 40% flow / 30% pressure / 20% thermal / 10% vibration");
            System.out.println("  - Failure mode diagnosis: 6 modes (scale, impeller, bearing, seal, GVF, cavitation)");
            System.out.println("  - ISO 10816-7 vibration zones A/B/C/D applied");
            System.out.println("  - PHI history recorded every batch → Redis time-series (sliding 50-reading window)");
            System.out.println("  - ML RUL: LinearRegression on PHI history for BEARING_FAILURE_RISK wells");
            System.out.println("  - Rule-based qualitative RUL for all other failure modes");
            System.out.println("  - L1-L4 alarms with rul_method flag (linear_regression vs rule_based) → Redis (10-min TTL)\n");

            // Both queries run concurrently. awaitAnyTermination() blocks until either stops.
            spark.streams().awaitAnyTermination();

        } catch (Exception e) {
            System.err.println("Pipeline error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
