-- =============================================================================
-- WellStream KSQL Streams — Real-Time Emission Monitoring
-- Regulatory basis: EPA 40 CFR Part 60 Subpart OOOOa (oil & gas methane rule)
-- =============================================================================

-- Base ingestion stream from Kafka topic (Avro format, Schema Registry enforced)
CREATE STREAM well_stream (
    well_id VARCHAR,
    facility_id VARCHAR,
    timestamp BIGINT,
    methane_ppm DOUBLE,
    co2_ppm DOUBLE,
    pressure_psi DOUBLE,
    flow_rate_bpd DOUBLE,
    pump_status VARCHAR,
    maintenance_alert BOOLEAN
) WITH (
    KAFKA_TOPIC='well-sensor-data',
    VALUE_FORMAT='AVRO',
    TIMESTAMP='timestamp'
);

-- Emission risk classification
-- Threshold: 500 ppm CH4 = equipment leak per EPA Method 21 (40 CFR Part 60 Subpart OOOOa)
CREATE STREAM well_enriched AS
SELECT
    well_id,
    facility_id,
    methane_ppm,
    co2_ppm,
    pressure_psi,
    flow_rate_bpd,
    pump_status,
    maintenance_alert,
    CASE
        WHEN methane_ppm > 500 THEN 'HIGH_EMISSION'   -- EPA Method 21 leak threshold
        WHEN methane_ppm > 300 THEN 'MEDIUM_EMISSION' -- Internal early-warning threshold
        ELSE 'NORMAL'
    END AS emission_risk_level
FROM well_stream
EMIT CHANGES;

-- =============================================================================
-- EPA Subpart OOOOa Exceedance Alert Stream
-- Emits downstream whenever a well breaches the federal methane leak threshold.
-- Downstream consumers: LDAR inspection scheduler, HSE notification service.
-- =============================================================================
CREATE STREAM epa_exceedance_alerts AS
SELECT
    well_id,
    facility_id,
    methane_ppm,
    emission_risk_level,
    'EPA_40CFR60_OOOOa' AS regulation_code,
    'LDAR_INSPECTION_REQUIRED' AS required_action,
    TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') AS detected_at
FROM well_enriched
WHERE emission_risk_level = 'HIGH_EMISSION'
EMIT CHANGES;

-- =============================================================================
-- Pump Maintenance Alert Stream
-- Surfaces wells with active maintenance_alert flag for preventive work orders
-- =============================================================================
CREATE STREAM pump_maintenance_alerts AS
SELECT
    well_id,
    facility_id,
    pump_status,
    pressure_psi,
    flow_rate_bpd,
    TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') AS flagged_at
FROM well_stream
WHERE maintenance_alert = TRUE
EMIT CHANGES;
