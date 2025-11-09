-- Axpo IoT Data Pipeline - Database Schema
-- PostgreSQL initialization script

-- ============================================
-- SENSORS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id VARCHAR(50) PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    min_value FLOAT NOT NULL,
    max_value FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- MEASUREMENTS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS measurements (
    id SERIAL,
    sensor_id VARCHAR(50) NOT NULL REFERENCES sensors(sensor_id),
    timestamp TIMESTAMP NOT NULL,
    value FLOAT NOT NULL,
    quality_flag VARCHAR(20) DEFAULT 'valid',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, timestamp)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_measurements_sensor_time 
    ON measurements(sensor_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_measurements_timestamp 
    ON measurements(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_measurements_quality 
    ON measurements(quality_flag) 
    WHERE quality_flag != 'valid';

-- ============================================
-- MEASUREMENTS_1MIN TABLE (Optional)
-- ============================================
CREATE TABLE IF NOT EXISTS measurements_1min (
    sensor_id VARCHAR(50) NOT NULL REFERENCES sensors(sensor_id),
    timestamp TIMESTAMP NOT NULL,
    avg_value FLOAT NOT NULL,
    min_value FLOAT NOT NULL,
    max_value FLOAT NOT NULL,
    sample_count INTEGER NOT NULL,
    PRIMARY KEY (sensor_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_measurements_1min_timestamp 
    ON measurements_1min(timestamp DESC);

-- ============================================
-- DATA_QUALITY TABLE (Optional)
-- ============================================
CREATE TABLE IF NOT EXISTS data_quality (
    sensor_id VARCHAR(50) NOT NULL REFERENCES sensors(sensor_id),
    check_timestamp TIMESTAMP NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value FLOAT NOT NULL,
    status VARCHAR(20) NOT NULL,
    PRIMARY KEY (sensor_id, check_timestamp, metric_name)
);

-- ============================================
-- INITIAL DATA - Matching generator's sensor IDs
-- ============================================
INSERT INTO sensors (sensor_id, location, sensor_type, unit, min_value, max_value)
VALUES
    ('Sensor 1', 'Datacenter Room A', 'temperature', 'C', 0.0, 50.0),
    ('Sensor 2', 'Datacenter Room B', 'temperature', 'C', 0.0, 50.0),
    ('Sensor 3', 'Server Rack 1', 'temperature', 'C', 0.0, 50.0),
    ('Sensor 4', 'Server Rack 2', 'temperature', 'C', 0.0, 50.0),
    ('Sensor 5', 'Cooling System', 'temperature', 'C', 0.0, 50.0)
ON CONFLICT (sensor_id) DO NOTHING;

-- ============================================
-- HELPER VIEWS
-- ============================================

-- Latest reading per sensor
CREATE OR REPLACE VIEW latest_readings AS
SELECT DISTINCT ON (sensor_id)
    s.sensor_id,
    s.location,
    s.sensor_type,
    m.timestamp,
    m.value,
    s.unit,
    m.quality_flag
FROM sensors s
JOIN measurements m ON s.sensor_id = m.sensor_id
ORDER BY sensor_id, m.timestamp DESC;

-- Hourly aggregations
CREATE OR REPLACE VIEW hourly_aggregates AS
SELECT 
    s.sensor_id,
    s.location,
    DATE_TRUNC('hour', m.timestamp) as hour,
    AVG(m.value) as avg_value,
    MIN(m.value) as min_value,
    MAX(m.value) as max_value,
    COUNT(*) as sample_count,
    ROUND(
        SUM(CASE WHEN m.quality_flag = 'valid' THEN 1 ELSE 0 END)::numeric / 
        COUNT(*)::numeric * 100, 
        2
    ) as valid_percentage
FROM sensors s
JOIN measurements m ON s.sensor_id = m.sensor_id
GROUP BY s.sensor_id, s.location, DATE_TRUNC('hour', m.timestamp);

-- ============================================
-- GRANTS
-- ============================================
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO iot_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO iot_user;

-- Analyze tables
ANALYZE sensors;
ANALYZE measurements;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✓ Database schema initialized';
    RAISE NOTICE '✓ 5 sensors loaded: Sensor 1-5';
    RAISE NOTICE '✓ Ready to collect data';
END $$;