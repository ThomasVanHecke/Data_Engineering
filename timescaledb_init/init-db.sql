CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE machine_sensors (
    time        TIMESTAMPTZ NOT NULL,
    device_id   INT         NOT NULL,
    device_type TEXT        NOT NULL,
    sensor_type TEXT        NOT NULL,
    sensor_val  DOUBLE PRECISION,
    location    TEXT        NULL,
    PRIMARY KEY (time, device_id)
);
SELECT create_hypertable('machine_sensors', 'time');
SELECT add_retention_policy('machine_sensors', INTERVAL '90 days');

CREATE TABLE sensor_aggregates (
    window_start        TIMESTAMPTZ NOT NULL,
    window_type TEXT        NOT NULL,
    device_id   INT         NOT NULL,
    device_type TEXT        NOT NULL,
    sensor_type TEXT        NOT NULL,
    average_val DOUBLE PRECISION NULL,
    min_val     DOUBLE PRECISION NULL,
    max_val     DOUBLE PRECISION NULL,
    count       INTEGER     NOT NULL,
    PRIMARY KEY (window_start, window_type, device_id, device_type, sensor_type)
);
SELECT create_hypertable('sensor_aggregates', 'window_start');
SELECT add_retention_policy('sensor_aggregates', INTERVAL '90 days');
