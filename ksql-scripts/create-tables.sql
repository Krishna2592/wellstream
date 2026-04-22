-- =============================================================================
-- WellStream KSQL Tables — Aggregated Compliance Reporting
-- =============================================================================

-- 5-second tumbling window: real-time methane KPIs per well
-- Used by Grafana dashboards for live operator view
CREATE TABLE well_realtime_kpis AS
SELECT
    well_id,
    WINDOWSTART AS window_start,
    WINDOWEND   AS window_end,
    AVG(methane_ppm) AS avg_methane_ppm,
    MAX(methane_ppm) AS peak_methane_ppm,
    MIN(methane_ppm) AS min_methane_ppm,
    COUNT(*)         AS reading_count
FROM well_stream
WINDOW TUMBLING (SIZE 5 SECONDS)
GROUP BY well_id
EMIT CHANGES;

-- =============================================================================
-- Hourly LDAR Compliance Summary
-- Leak Detection and Repair (LDAR) program reporting per EPA 40 CFR Part 60
-- Retention: 1 year (federal record-keeping requirement)
-- =============================================================================
CREATE TABLE ldar_hourly_compliance AS
SELECT
    well_id,
    WINDOWSTART AS hour_start,
    AVG(methane_ppm) AS avg_methane_ppm,
    MAX(methane_ppm) AS peak_methane_ppm,
    COUNT(*)         AS total_readings
FROM well_stream
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY well_id
EMIT FINAL;

-- =============================================================================
-- Facility-Level Emission Roll-Up (24-hour)
-- Aggregates all wells at a facility for ESG reporting and permit compliance
-- =============================================================================
CREATE TABLE facility_daily_emissions AS
SELECT
    facility_id,
    WINDOWSTART AS day_start,
    AVG(methane_ppm)  AS avg_facility_methane_ppm,
    MAX(methane_ppm)  AS peak_facility_methane_ppm,
    COUNT(*)          AS total_sensor_readings,
    COUNT(DISTINCT well_id) AS active_wells
FROM well_stream
WINDOW TUMBLING (SIZE 24 HOURS)
GROUP BY facility_id
EMIT FINAL;
