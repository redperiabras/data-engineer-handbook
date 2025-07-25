-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);

DROP TABLE IF EXISTS processed_events_aggregated_source;
CREATE TABLE processed_events_aggregated_source (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
)

DROP TABLE IF EXISTS processed_events_aggregated;
CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
)
