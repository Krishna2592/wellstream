package com.wellstream.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ML-based Remaining Useful Life (RUL) predictor for BEARING_FAILURE_RISK failure mode.
 *
 * How it works:
 *   1. Every foreachBatch cycle, the current Pump Health Index (PHI) is recorded
 *      to a per-well Redis list (time-series store).
 *   2. Once MIN_HISTORY_POINTS readings are available, Spark MLlib LinearRegression
 *      is fit on the PHI time-series: PHI(t) = slope * t + intercept.
 *   3. The regression is extrapolated to find t_critical, where PHI reaches the
 *      critical threshold (25.0). That gives RUL = t_critical - t_now.
 *   4. R² is reported alongside the prediction so the consumer knows how reliable
 *      the trend estimate is.
 *
 * Why LinearRegression for bearings specifically:
 *   Bearing wear produces a monotonically increasing temperature and vibration load,
 *   causing PHI to degrade linearly with time in the early-to-mid failure stages
 *   (per ISO 13374-1 prognostics methodology and SPE-181704 ESP field data).
 *   Non-linear failure modes (impeller wear, scale buildup) are better served by
 *   physics models — covered by PumpHealthMonitor's rule-based diagnosis.
 *
 * Production evolution path (noted in code comments):
 *   - Replace Redis list with Delta Lake history table for persistent, queryable history
 *   - Upgrade LinearRegression to polynomial or exponential regression for late-stage bearing
 *   - Register trained models in MLflow Model Registry for versioning and A/B testing
 *   - Add confidence intervals using bootstrap sampling on the history window
 */
public class BearingRulPredictor {

    private static final int    MIN_HISTORY_POINTS = 5;
    private static final int    MAX_HISTORY_POINTS = 50;   // ~8 minutes at 10s trigger
    private static final double CRITICAL_PHI       = PumpHealthMonitor.PHI_CRITICAL;  // 25.0
    private static final String KEY_PREFIX         = "pump:rul:bearing:";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // -------------------------------------------------------------------------
    // Record a PHI reading to the well's time-series in Redis.
    // Called every batch for ALL active pumps (not just bearing failures) so
    // history accumulates before a fault is diagnosed.
    //
    // Redis structure:
    //   Key:   pump:rul:bearing:{well_id}
    //   Type:  List of JSON strings  [{"phi":85.2,"ts":1714567890123}, ...]
    //   TTL:   3600s — resets on every write; expires after 1h of inactivity
    // -------------------------------------------------------------------------
    public static void recordPhiHistory(String wellId, double phi, long timestampMs, Jedis jedis) {
        try {
            String key   = KEY_PREFIX + wellId;
            String entry = String.format("{\"phi\":%.4f,\"ts\":%d}", phi, timestampMs);
            jedis.rpush(key, entry);
            jedis.ltrim(key, -MAX_HISTORY_POINTS, -1);  // keep last N readings (sliding window)
            jedis.expire(key, 3600);
        } catch (Exception e) {
            // Non-critical — history recording failure does not block pipeline
            System.err.println("PHI history record skipped for " + wellId + ": " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Retrieve PHI history length (used to decide whether prediction is possible).
    // -------------------------------------------------------------------------
    public static int getHistoryLength(String wellId, Jedis jedis) {
        try {
            return (int) jedis.llen(KEY_PREFIX + wellId);
        } catch (Exception e) {
            return 0;
        }
    }

    // -------------------------------------------------------------------------
    // Predict RUL using LinearRegression on the PHI history stored in Redis.
    //
    // Returns a human-readable string, e.g.:
    //   "~18.4 hours (R²=0.94, slope=-3.21 PHI/hr)"
    //   "collecting history (3/5 readings)"
    //   "stable/improving (slope=+0.42 PHI/hr)"
    //   "OVERDUE — model projected failure 2.1 hrs ago"
    //
    // The SparkSession is used to create the mini training DataFrame.
    // In foreachBatch, use batchDF.sparkSession() to obtain it.
    //
    // Production note: replace Redis lrange with a Delta Lake read:
    //   spark.read.format("delta").load("/mnt/phi-history").filter("well_id = 'WELL-18'")
    // -------------------------------------------------------------------------
    public static String predictRul(String wellId, Jedis jedis, SparkSession spark) {
        List<String> rawHistory = jedis.lrange(KEY_PREFIX + wellId, 0, -1);

        if (rawHistory.size() < MIN_HISTORY_POINTS) {
            return String.format("collecting history (%d/%d readings)",
                rawHistory.size(), MIN_HISTORY_POINTS);
        }

        try {
            // Parse history into (timeOffsetHours, phi) pairs
            // Time is expressed as hours from the first reading so LinearRegression
            // works on a human-readable scale rather than epoch milliseconds.
            long   firstTs     = 0;
            List<double[]> points = new ArrayList<>();

            for (String entry : rawHistory) {
                Map<?, ?> parsed = MAPPER.readValue(entry, Map.class);
                double phi = ((Number) parsed.get("phi")).doubleValue();
                long   ts  = ((Number) parsed.get("ts")).longValue();
                if (firstTs == 0) firstTs = ts;
                double tHours = (ts - firstTs) / 3_600_000.0;
                points.add(new double[]{tHours, phi});
            }

            // Build a small Spark DataFrame: feature = time (hours), label = PHI
            List<Row> rows = new ArrayList<>();
            for (double[] p : points) {
                rows.add(RowFactory.create(Vectors.dense(p[0]), p[1]));
            }

            StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(),       false, Metadata.empty()),
                new StructField("phi",      DataTypes.DoubleType,  false, Metadata.empty())
            });
            Dataset<Row> historyDF = spark.createDataFrame(rows, schema);

            // Fit LinearRegression: PHI(t) = slope * t + intercept
            // No regularization — we want an exact fit on this small, clean history window.
            // Production: add L2 regularization (regParam) once the history window is larger.
            LinearRegression lr = new LinearRegression()
                .setFeaturesCol("features")
                .setLabelCol("phi")
                .setMaxIter(100)
                .setRegParam(0.0)
                .setElasticNetParam(0.0);

            LinearRegressionModel model    = lr.fit(historyDF);
            double                slope     = model.coefficients().toArray()[0]; // PHI change per hour
            double                intercept = model.intercept();
            double                r2        = model.summary().r2();

            // Negative slope required — bearing degradation is monotonically worsening
            if (slope >= 0.0) {
                return String.format("stable or improving (slope=+%.3f PHI/hr, R²=%.2f)", slope, r2);
            }

            // Extrapolate: solve PHI(t) = CRITICAL_PHI for t
            // t_critical = (CRITICAL_PHI - intercept) / slope
            double tNowHours    = (System.currentTimeMillis() - firstTs) / 3_600_000.0;
            double tCritical    = (CRITICAL_PHI - intercept) / slope;
            double hoursToFail  = tCritical - tNowHours;

            if (hoursToFail <= 0.0) {
                return String.format(
                    "OVERDUE — model projected PHI critical %.1f hrs ago (R²=%.2f, slope=%.3f PHI/hr)",
                    Math.abs(hoursToFail), r2, slope);
            }

            // Format RUL in the most human-readable unit
            String rulFormatted;
            if      (hoursToFail <  1.0)  rulFormatted = String.format("~%.0f min",  hoursToFail * 60);
            else if (hoursToFail < 48.0)  rulFormatted = String.format("~%.1f hrs",  hoursToFail);
            else                          rulFormatted = String.format("~%.1f days",  hoursToFail / 24.0);

            return String.format(
                "%s to critical (LinearRegression: slope=%.3f PHI/hr, R²=%.2f, n=%d)",
                rulFormatted, slope, r2, points.size());

        } catch (Exception e) {
            return "prediction unavailable: " + e.getMessage();
        }
    }
}
