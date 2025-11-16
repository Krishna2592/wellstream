-- Create stream from Kafka topic
CREATE STREAM well_stream (
    well_id VARCHAR,
    methane_ppm DOUBLE,
    pressure_bar DOUBLE
) WITH (
    KAFKA_TOPIC='well-sensor-data',
    VALUE_FORMAT='AVRO'
);

-- Create enriched stream with risk classification
CREATE STREAM well_enriched AS
SELECT
    well_id,
    methane_ppm,
    CASE
        WHEN methane_ppm > 500 THEN 'HIGH_EMISSION'
        WHEN methane_ppm > 300 THEN 'MEDIUM_EMISSION'
        ELSE 'NORMAL'
    END AS risk_level
FROM well_stream
EMIT CHANGES;