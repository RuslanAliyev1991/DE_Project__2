CREATE SCHEMA if not exists raw;
CREATE SCHEMA if not exists silver;

CREATE TABLE IF NOT EXISTS raw.kafka_raw_flights (
    id SERIAL PRIMARY KEY,
    payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS silver.flight_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    active_flights_count BIGINT,
    average_altitude DOUBLE PRECISION
);

-- 2. Anomaliyalar Cədvəli (Təhlükəli eniş/qalxış edənlər)
CREATE TABLE IF NOT EXISTS silver.flight_anomalies (
    timestamp TIMESTAMP,
    icao24 VARCHAR(50),
    callsign VARCHAR(50),
    vertical_rate DOUBLE PRECISION
);

-- 3. İstilik Xəritəsi Üçün Koordinat Cədvəli
CREATE TABLE IF NOT EXISTS silver.flight_coordinates (
    timestamp TIMESTAMP,
    icao24 VARCHAR(50),
    callsign VARCHAR(50),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);