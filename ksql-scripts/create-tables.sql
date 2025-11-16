-- Aggregate by 5-second tumbling window
CREATE TABLE well_aggregates AS
SELECT
    well_id,
    WINDOWSTART AS window_start,
    AVG(methane_ppm) AS avg_methane,
    MAX(methane_ppm) AS max_methane
FROM well_stream
WINDOW TUMBLING (SIZE 5 SECONDS)
GROUP BY well_id
EMIT CHANGES;