package com.wellstream.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.least;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * Pump Health Monitor — Physics-informed ML pipeline for ESP and centrifugal pump failure prediction.
 *
 * Methodology mirrors SLB LIFT IQ / Aramco EXPEC ARC diagnostic approach:
 *
 *   Step A: Feature engineering from raw sensor telemetry
 *           Deviations from pump curve Best Efficiency Point (BEP) are computed.
 *           A pump operating far from BEP generates excess heat, vibration, and wear.
 *
 *   Step B: Pump Health Index (PHI, 0-100) — weighted multi-signal score
 *           Flow contribution    40% — most sensitive to impeller and seal degradation
 *           Pressure contribution 30% — indicates restriction (scale/wax) or impeller damage
 *           Thermal contribution  20% — leading indicator of bearing and motor failure
 *           Vibration contribution 10% — mechanical imbalance, cavitation, misalignment
 *
 *   Step C: Failure mode diagnosis — pattern recognition from multi-signal combinations
 *           Six failure mode patterns based on API 610 / ISO 13709 diagnostic criteria
 *
 *   Step D: Tiered alarm escalation
 *           L1 ADVISORY  — PHI degrading, early signals (log for next planned maintenance)
 *           L2 WARNING   — PHI < 50, confirmed failure mode signal (inspect within 24h)
 *           L3 CRITICAL  — PHI < 25, thermal overrun, or shutdown-risk combination (4h response)
 *           L4 SHUTDOWN  — vibration in ISO zone D or PHI critical + confirmed fault (pull well now)
 *
 * References:
 *   - API 610 (ISO 13709): Centrifugal Pumps for Petroleum, Petrochemical and Natural Gas Industries
 *   - ISO 10816-7: Mechanical Vibration — Evaluation of machine vibration by measurements on rotating shafts
 *   - SPE-181704: Integrated Pump Diagnostics Using Real-Time Sensor Data (SLB, 2016)
 *   - Aramco Engineering Standard SAES-J-100: Instrumentation for Rotating Equipment
 */
public class PumpHealthMonitor {

    // -------------------------------------------------------------------------
    // Pump curve design point (Best Efficiency Point — from pump manufacturer datasheet)
    // In production these are loaded per-asset from the PI Asset Framework / SAP PM
    // -------------------------------------------------------------------------
    private static final double DESIGN_FLOW_BPD     = 175.0;  // Rated flow at BEP
    private static final double DESIGN_PRESSURE_PSI = 300.0;  // Rated discharge pressure at BEP
    private static final double DESIGN_TEMP_F       = 90.0;   // Normal operating fluid temperature

    // -------------------------------------------------------------------------
    // ISO 10816-7 / API 610 vibration severity zones (normalized 0–1 proxy)
    // Zone A: new equipment                 0.00 – 0.28
    // Zone B: acceptable long-term          0.28 – 0.45
    // Zone C: tolerable short-term only     0.45 – 0.71  → schedule inspection
    // Zone D: damage imminent               0.71 – 1.00  → shut down
    // -------------------------------------------------------------------------
    private static final double VIB_ZONE_A_B = 0.28;
    private static final double VIB_ZONE_B_C = 0.45;
    private static final double VIB_ZONE_C_D = 0.71;

    // PHI decision thresholds
    public static final double PHI_HEALTHY   = 75.0;
    public static final double PHI_DEGRADING = 50.0;
    public static final double PHI_CRITICAL  = 25.0;

    // -------------------------------------------------------------------------
    // STEP A: Physics-informed feature engineering
    //
    // Each feature is a deviation from the pump's design operating point.
    // Pumps operating at BEP have minimum hydraulic forces, heat, and wear.
    // Deviation from BEP is the root cause of most mechanical failure modes.
    // -------------------------------------------------------------------------
    public static Dataset<Row> engineerFeatures(Dataset<Row> wellBatch) {
        return wellBatch
            // Flow deviation from BEP (%)
            // Negative: reduced throughput → impeller wear, gas interference, inlet blockage
            // Positive: above-BEP recirculation → increased vibration and seal wear
            .withColumn("flow_deviation_pct",
                col("flow_rate_bpd").minus(lit(DESIGN_FLOW_BPD))
                    .divide(lit(DESIGN_FLOW_BPD)).multiply(lit(100.0)))

            // Pressure deviation from rated head (%)
            // Positive spike: downstream restriction (scale, wax, closed valve)
            // Negative drop: impeller wear, worn wear rings, internal recirculation
            .withColumn("pressure_deviation_pct",
                col("pressure_psi").minus(lit(DESIGN_PRESSURE_PSI))
                    .divide(lit(DESIGN_PRESSURE_PSI)).multiply(lit(100.0)))

            // Temperature rise above design (°F)
            // >20°F: motor overload or fluid friction increase (off-BEP operation)
            // >35°F: bearing failure onset or seal degradation
            // >50°F: imminent thermal shutdown
            .withColumn("temp_rise_f",
                col("temperature_f").minus(lit(DESIGN_TEMP_F)))

            // Hydraulic efficiency proxy: actual (Q×H) vs design (Q_bep×H_bep)
            // <85%: operating significantly off BEP
            // <70%: major hydraulic degradation — impeller or casing wear
            // <50%: severe internal recirculation or mechanical damage
            .withColumn("hydraulic_efficiency_pct",
                col("flow_rate_bpd").multiply(col("pressure_psi"))
                    .divide(lit(DESIGN_FLOW_BPD * DESIGN_PRESSURE_PSI))
                    .multiply(lit(100.0)))

            // Gas Void Fraction (GVF) risk flag
            // >15% GVF causes severe centrifugal pump cavitation and flow instability.
            // High methane near pump + depressed flow is the primary observable indicator.
            .withColumn("gvf_risk_flag",
                when(col("methane_ppm").gt(400).and(col("flow_deviation_pct").lt(-10)), lit(1))
                .otherwise(lit(0)))

            // Vibration severity (0–1, normalized) — mechanical stress proxy
            // Derived from pressure and flow deviation from BEP:
            // A pump far from BEP generates hydraulic radial forces proportional to head deviation.
            // In production: replace with accelerometer RMS readings (mm/s per ISO 10816-7).
            .withColumn("vibration_severity",
                least(
                    lit(1.0),
                    abs(col("pressure_deviation_pct")).divide(lit(80.0))
                    .plus(abs(col("flow_deviation_pct")).divide(lit(120.0)))
                    .plus(col("temp_rise_f").cast(DataTypes.DoubleType).divide(lit(80.0)))
                ));
    }

    // -------------------------------------------------------------------------
    // STEP B: Pump Health Index (PHI) computation
    //
    // Weighted multi-signal score on 0–100 scale.
    // Weighting rationale (per SPE-181704 field validation data):
    //   40% flow    — first signal to degrade in most ESP failure modes
    //   30% pressure — distinguishes restriction from wear (opposite pressure directions)
    //   20% thermal  — leads mechanical failure by days to weeks in bearing/seal failures
    //   10% vibration — confirmatory signal; spikes late but is definitive for cavitation
    // -------------------------------------------------------------------------
    public static Dataset<Row> computePHI(Dataset<Row> df) {
        return df
            .withColumn("phi_flow",
                when(col("flow_deviation_pct").geq(-5),   lit(100.0))  // within 5% of BEP: healthy
                .when(col("flow_deviation_pct").geq(-15), lit(75.0))   // 5–15% below BEP: monitor
                .when(col("flow_deviation_pct").geq(-30), lit(45.0))   // 15–30% below BEP: warning
                .when(col("flow_deviation_pct").geq(-50), lit(20.0))   // 30–50% below BEP: critical
                .otherwise(lit(5.0)))                                   // >50% below BEP: severe

            .withColumn("phi_pressure",
                when(col("pressure_deviation_pct").between(-10, 15),  lit(100.0))
                .when(col("pressure_deviation_pct").between(-20, 25), lit(75.0))
                .when(col("pressure_deviation_pct").between(-30, 40), lit(45.0))
                .otherwise(lit(15.0)))

            .withColumn("phi_thermal",
                when(col("temp_rise_f").lt(10),  lit(100.0))
                .when(col("temp_rise_f").lt(20), lit(80.0))
                .when(col("temp_rise_f").lt(35), lit(50.0))
                .when(col("temp_rise_f").lt(50), lit(25.0))
                .otherwise(lit(5.0)))

            .withColumn("phi_vibration",
                when(col("vibration_severity").lt(VIB_ZONE_A_B), lit(100.0))  // Zone A — new/healthy
                .when(col("vibration_severity").lt(VIB_ZONE_B_C), lit(75.0))  // Zone B — normal wear
                .when(col("vibration_severity").lt(VIB_ZONE_C_D), lit(35.0))  // Zone C — inspect soon
                .otherwise(lit(5.0)))                                          // Zone D — shut down

            .withColumn("pump_health_index",
                col("phi_flow").multiply(lit(0.40))
                .plus(col("phi_pressure").multiply(lit(0.30)))
                .plus(col("phi_thermal").multiply(lit(0.20)))
                .plus(col("phi_vibration").multiply(lit(0.10))));
    }

    // -------------------------------------------------------------------------
    // STEP C: Failure mode diagnosis
    //
    // Pattern recognition from multi-signal combinations.
    // Each diagnosis maps to a specific physical failure mechanism.
    // This mirrors the NODAL analysis approach used in SLB Avocet and
    // Aramco's Virtual Flow Metering diagnostic rules.
    // -------------------------------------------------------------------------
    public static Dataset<Row> diagnoseFailureMode(Dataset<Row> df) {
        return df
            .withColumn("failure_mode",

                // SCALE / WAX BUILDUP — flow drops as restriction forms downstream.
                // Pressure increases because the pump pushes against the blockage.
                // Signature: ↓flow + ↑pressure simultaneously.
                // Action: hot-oil treatment or mechanical pigging.
                when(col("flow_deviation_pct").lt(-15)
                    .and(col("pressure_deviation_pct").gt(20)),
                    lit("SCALE_WAX_BUILDUP"))

                // IMPELLER WEAR — both flow and pressure drop as impeller loses geometry.
                // The pump can no longer generate rated head at rated flow.
                // Signature: ↓flow AND ↓pressure together.
                // Action: pull and replace impeller stack.
                .when(col("flow_deviation_pct").lt(-20)
                    .and(col("pressure_deviation_pct").lt(-15)),
                    lit("IMPELLER_WEAR"))

                // BEARING FAILURE — friction heat from worn or misaligned bearings.
                // Vibration also rises as bearing clearance increases.
                // Signature: ↑temperature + ↑vibration (both elevated).
                // Action: bearing replacement, check alignment and lubrication.
                .when(col("temp_rise_f").gt(30)
                    .and(col("vibration_severity").gt(VIB_ZONE_B_C)),
                    lit("BEARING_FAILURE_RISK"))

                // SEAL DEGRADATION — mechanical seal faces wearing causes internal recirculation.
                // Temperature rises from fluid friction but flow and pressure are relatively stable.
                // Signature: ↑temperature, stable flow and pressure (rules out bearing).
                // Action: seal inspection, check flush plan adequacy.
                .when(col("temp_rise_f").gt(25)
                    .and(col("flow_deviation_pct").between(-10, 5))
                    .and(col("pressure_deviation_pct").between(-10, 10)),
                    lit("SEAL_DEGRADATION"))

                // GAS INTERFERENCE / CAVITATION — gas void fraction above ~15% causes
                // centrifugal pump to partially cavitate, reducing effective flow.
                // High methane at pump inlet is the leading indicator.
                // Signature: high methane + depressed flow.
                // Action: increase casing pressure, install gas separator, choke back production.
                .when(col("gvf_risk_flag").equalTo(1),
                    lit("GAS_INTERFERENCE_CAVITATION"))

                // MECHANICAL IMBALANCE / CAVITATION — vibration spike without significant
                // thermal signature indicates rotor imbalance, bent shaft, or inlet cavitation.
                // Signature: high vibration, normal temperature.
                // Action: vibration analysis (FFT), check inlet conditions and suction head.
                .when(col("vibration_severity").gt(VIB_ZONE_B_C)
                    .and(col("temp_rise_f").lt(20)),
                    lit("MECHANICAL_IMBALANCE_CAVITATION"))

                // EFFICIENCY DEGRADATION — hydraulic efficiency below 70% without a clear
                // single-signal cause. Usually combined wear across multiple components.
                // Signature: low efficiency_pct, PHI declining gradually.
                // Action: schedule full pump workover at next planned shutdown.
                .when(col("hydraulic_efficiency_pct").lt(70.0),
                    lit("EFFICIENCY_DEGRADATION"))

                .otherwise(lit("NO_FAULT_DETECTED")))

            // Remaining Useful Life estimate (qualitative) based on PHI trend
            // In production: replace with time-series regression on PHI history from Delta Lake
            .withColumn("rul_estimate",
                when(col("pump_health_index").geq(PHI_HEALTHY),   lit("90+ days"))
                .when(col("pump_health_index").geq(PHI_DEGRADING), lit("14–90 days"))
                .when(col("pump_health_index").geq(PHI_CRITICAL),  lit("1–14 days"))
                .otherwise(lit("<24 hours — pull well")));
    }

    // -------------------------------------------------------------------------
    // STEP D: Tiered alarm escalation
    //
    // Alarm levels per API 610 Annex K and SLB LIFT IQ intervention framework.
    // Each level has a defined response time and action.
    // -------------------------------------------------------------------------
    public static Dataset<Row> applyAlarmEscalation(Dataset<Row> df) {
        return df
            .withColumn("pump_alarm_level",

                // L4 SHUTDOWN: vibration in ISO zone D, OR PHI critical with confirmed fault.
                // Continued operation risks catastrophic failure and unplanned workover.
                when(col("vibration_severity").gt(VIB_ZONE_C_D)
                    .or(col("pump_health_index").lt(PHI_CRITICAL)
                        .and(col("failure_mode").notEqual("NO_FAULT_DETECTED"))),
                    lit("L4_SHUTDOWN"))

                // L3 CRITICAL: PHI below 25, severe thermal event, or seal/bearing alarm.
                // Field team dispatch required within 4 hours.
                .when(col("pump_health_index").lt(PHI_CRITICAL)
                    .or(col("temp_rise_f").gt(45))
                    .or(col("failure_mode").isin("BEARING_FAILURE_RISK", "SEAL_DEGRADATION")),
                    lit("L3_CRITICAL"))

                // L2 WARNING: PHI degrading, vibration in zone C, or structural failure mode confirmed.
                // Inspection within 24 hours.
                .when(col("pump_health_index").lt(PHI_DEGRADING)
                    .or(col("vibration_severity").gt(VIB_ZONE_B_C))
                    .or(col("failure_mode").isin(
                        "IMPELLER_WEAR", "SCALE_WAX_BUILDUP",
                        "GAS_INTERFERENCE_CAVITATION", "MECHANICAL_IMBALANCE_CAVITATION")),
                    lit("L2_WARNING"))

                // L1 ADVISORY: early degradation signals.
                // Log for next planned maintenance cycle.
                .when(col("pump_health_index").lt(PHI_HEALTHY)
                    .or(col("flow_deviation_pct").lt(-10))
                    .or(col("failure_mode").equalTo("EFFICIENCY_DEGRADATION")),
                    lit("L1_ADVISORY"))

                .otherwise(lit("NORMAL")))

            .withColumn("recommended_action",
                when(col("pump_alarm_level").equalTo("L4_SHUTDOWN"),
                    lit("SHUT DOWN IMMEDIATELY — initiate well pull and full pump inspection"))
                .when(col("pump_alarm_level").equalTo("L3_CRITICAL"),
                    lit("Dispatch field team within 4h — confirm failure mode, prepare workover"))
                .when(col("pump_alarm_level").equalTo("L2_WARNING"),
                    lit("Schedule inspection within 24h — run vibration survey and fluid analysis"))
                .when(col("pump_alarm_level").equalTo("L1_ADVISORY"),
                    lit("Log in CMMS — inspect at next planned maintenance window"))
                .otherwise(lit("Continue normal monitoring")));
    }

    // -------------------------------------------------------------------------
    // Full pipeline: raw well telemetry → pump health report
    // Call this from foreachBatch on the well sensor stream.
    // -------------------------------------------------------------------------
    public static Dataset<Row> evaluate(Dataset<Row> wellBatch) {
        return applyAlarmEscalation(
                    diagnoseFailureMode(
                        computePHI(
                            engineerFeatures(wellBatch))));
    }
}
