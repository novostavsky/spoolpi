-- SpoolPi's reference Postgres schema. Safe to apply repeatedly.
--
-- Delivery from SpoolPi is at-least-once, so every record is keyed by
-- (buffer_id, seq) and inserted with ON CONFLICT DO NOTHING: a resend is a no-op.
--
-- Time: wall_ns is what the device's clock said, corrected after NTP sync when
-- possible. Trust it (and `ts`) only where ts_quality > 0; ts_quality = 0 rows
-- were sampled before the clock synced and may say 1970. Order a device's
-- records by (buffer_id, seq), not by time: clocks step, seq doesn't.

CREATE TABLE IF NOT EXISTS spoolpi_readings (
    buffer_id   uuid             NOT NULL,
    seq         bigint           NOT NULL CHECK (seq >= 0),
    device_id   text             NOT NULL,
    sensor_id   text             NOT NULL,
    value       double precision,            -- NULL: the sensor read failed
    unit        text,
    mono_ns     bigint           NOT NULL,   -- CLOCK_BOOTTIME, meaningful within boot_id
    wall_ns     bigint           NOT NULL,   -- Unix epoch nanoseconds
    ts          timestamptz      GENERATED ALWAYS AS (to_timestamp(wall_ns::double precision / 1e9)) STORED,
    boot_id     text             NOT NULL,
    ts_quality  smallint         NOT NULL CHECK (ts_quality IN (0, 1, 2)),  -- unsynced / corrected / synced
    qc_flag     smallint         NOT NULL,   -- QARTOD; 2 = not evaluated
    qc_tests    text[]           NOT NULL DEFAULT '{}',
    received_at timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (buffer_id, seq)
);

CREATE INDEX IF NOT EXISTS spoolpi_readings_by_sensor_time
    ON spoolpi_readings (device_id, sensor_id, ts);

-- Readings a device discarded or a sink refused: `count` is exact, and every one
-- of them was sampled within [from_mono_ns, to_mono_ns] of boot_id.
CREATE TABLE IF NOT EXISTS spoolpi_gaps (
    buffer_id    uuid        NOT NULL,
    seq          bigint      NOT NULL CHECK (seq >= 0),
    device_id    text        NOT NULL,
    sensor_id    text,                      -- NULL: all sensors
    boot_id      text        NOT NULL,
    from_mono_ns bigint      NOT NULL,
    to_mono_ns   bigint      NOT NULL,
    reason       text        NOT NULL,      -- retention:drop_oldest | backpressure | rejected:sink
    count        integer     NOT NULL CHECK (count > 0),
    received_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (buffer_id, seq)
);

-- Messages that could never be stored (not JSON, missing or invalid fields).
-- Kept rather than dropped, so nothing disappears without a trace.
CREATE TABLE IF NOT EXISTS spoolpi_dead_letters (
    id          bigserial   PRIMARY KEY,
    source      text        NOT NULL,       -- e.g. the MQTT topic
    payload     bytea       NOT NULL,
    error       text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
