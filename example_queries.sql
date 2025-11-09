-- ============================================
-- Axpo IoT Pipeline - Example SQL Queries
-- For BI Analysts and Data Scientists
-- ============================================

-- ============================================
-- BASIC QUERIES
-- ============================================

-- 1. Get all sensors
SELECT * FROM sensors ORDER BY sensor_id;

-- 2. Latest reading per sensor
SELECT DISTINCT ON (sensor_id)
    sensor_id,
    timestamp,
    value,
    quality_flag
FROM measurements
ORDER BY sensor_id, timestamp DESC;

-- 3. Count measurements per sensor
SELECT 
    s.sensor_id,
    s.location,
    COUNT(m.id) as total_measurements,
    MIN(m.timestamp) as first_reading,
    MAX(m.timestamp) as last_reading
FROM sensors s
LEFT JOIN measurements m ON s.sensor_id = m.sensor_id
GROUP BY s.sensor_id, s.location;

-- 4. Data quality overview
SELECT 
    quality_flag,
    COUNT(*) as count,
    ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER () * 100, 2) as percentage
FROM measurements
GROUP BY quality_flag;

-- ============================================
-- BI ANALYST QUERIES
-- ============================================

-- 5. Last 24 hours average temperature by location
SELECT 
    s.location,
    DATE_TRUNC('hour', m.timestamp) as hour,
    AVG(m.value) as avg_temp,
    MIN(m.value) as min_temp,
    MAX(m.value) as max_temp,
    COUNT(*) as sample_count
FROM measurements m
JOIN sensors s ON m.sensor_id = s.sensor_id
WHERE m.timestamp >= NOW() - INTERVAL '24 hours'
    AND m.quality_flag = 'valid'
GROUP BY s.location, DATE_TRUNC('hour', m.timestamp)
ORDER BY hour DESC, s.location;

-- 6. Current vs average temperature per sensor
WITH current_stats AS (
    SELECT 
        sensor_id,
        AVG(value) as current_avg
    FROM measurements
    WHERE timestamp >= NOW() - INTERVAL '10 minutes'
        AND quality_flag = 'valid'
    GROUP BY sensor_id
),
historical_stats AS (
    SELECT 
        sensor_id,
        AVG(value) as historical_avg
    FROM measurements
    WHERE timestamp >= NOW() - INTERVAL '24 hours'
        AND quality_flag = 'valid'
    GROUP BY sensor_id
)
SELECT 
    s.sensor_id,
    s.location,
    ROUND(c.current_avg::numeric, 2) as current_temp,
    ROUND(h.historical_avg::numeric, 2) as day_avg,
    ROUND((c.current_avg - h.historical_avg)::numeric, 2) as difference
FROM sensors s
JOIN current_stats c ON s.sensor_id = c.sensor_id
JOIN historical_stats h ON s.sensor_id = h.sensor_id;

-- 7. Peak values per sensor (last 7 days)
SELECT 
    s.sensor_id,
    s.location,
    MAX(m.value) as peak_value,
    MIN(m.value) as lowest_value,
    AVG(m.value) as mean_value,
    STDDEV(m.value) as std_dev
FROM measurements m
JOIN sensors s ON m.sensor_id = s.sensor_id
WHERE m.timestamp >= NOW() - INTERVAL '7 days'
    AND m.quality_flag = 'valid'
GROUP BY s.sensor_id, s.location
ORDER BY peak_value DESC;

-- ============================================
-- DATA SCIENTIST QUERIES
-- ============================================

-- 8. Anomaly detection using Z-score
WITH sensor_stats AS (
    SELECT 
        sensor_id,
        AVG(value) as mean_value,
        STDDEV(value) as std_dev
    FROM measurements
    WHERE timestamp >= NOW() - INTERVAL '7 days'
        AND quality_flag = 'valid'
    GROUP BY sensor_id
)
SELECT 
    m.sensor_id,
    m.timestamp,
    m.value,
    s.mean_value,
    s.std_dev,
    ABS(m.value - s.mean_value) / s.std_dev as z_score
FROM measurements m
JOIN sensor_stats s ON m.sensor_id = s.sensor_id
WHERE ABS(m.value - s.mean_value) > 3 * s.std_dev
    AND m.timestamp >= NOW() - INTERVAL '7 days'
ORDER BY z_score DESC
LIMIT 20;

-- 9. Rate of change analysis
WITH value_changes AS (
    SELECT 
        sensor_id,
        timestamp,
        value,
        LAG(value) OVER (PARTITION BY sensor_id ORDER BY timestamp) as prev_value,
        LAG(timestamp) OVER (PARTITION BY sensor_id ORDER BY timestamp) as prev_timestamp
    FROM measurements
    WHERE timestamp >= NOW() - INTERVAL '1 hour'
        AND quality_flag = 'valid'
)
SELECT 
    sensor_id,
    timestamp,
    value,
    prev_value,
    (value - prev_value) as change,
    EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as seconds_elapsed,
    (value - prev_value) / EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as rate_per_second
FROM value_changes
WHERE prev_value IS NOT NULL
    AND ABS(value - prev_value) > 0.5
ORDER BY ABS(value - prev_value) DESC
LIMIT 20;

-- 10. Find data gaps
WITH time_series AS (
    SELECT 
        sensor_id,
        timestamp,
        LEAD(timestamp) OVER (PARTITION BY sensor_id ORDER BY timestamp) as next_timestamp
    FROM measurements
    WHERE timestamp >= NOW() - INTERVAL '1 hour'
)
SELECT 
    sensor_id,
    timestamp as gap_start,
    next_timestamp as gap_end,
    EXTRACT(EPOCH FROM (next_timestamp - timestamp)) as gap_seconds
FROM time_series
WHERE EXTRACT(EPOCH FROM (next_timestamp - timestamp)) > 5
ORDER BY gap_seconds DESC;

-- 11. Sensor correlation analysis
SELECT 
    m1.sensor_id as sensor_1,
    m2.sensor_id as sensor_2,
    CORR(m1.value, m2.value) as correlation,
    COUNT(*) as sample_size
FROM measurements m1
JOIN measurements m2 ON m1.timestamp = m2.timestamp
    AND m1.sensor_id < m2.sensor_id
WHERE m1.timestamp >= NOW() - INTERVAL '24 hours'
    AND m1.quality_flag = 'valid'
    AND m2.quality_flag = 'valid'
GROUP BY m1.sensor_id, m2.sensor_id
HAVING COUNT(*) > 100
ORDER BY correlation DESC;

-- 12. Moving average (7-day window)
SELECT 
    sensor_id,
    DATE_TRUNC('day', timestamp) as day,
    AVG(value) as daily_avg,
    AVG(AVG(value)) OVER (
        PARTITION BY sensor_id 
        ORDER BY DATE_TRUNC('day', timestamp)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7day
FROM measurements
WHERE timestamp >= NOW() - INTERVAL '30 days'
    AND quality_flag = 'valid'
GROUP BY sensor_id, DATE_TRUNC('day', timestamp)
ORDER BY sensor_id, day DESC;

-- ============================================
-- DATA QUALITY QUERIES
-- ============================================

-- 13. Data completeness per sensor (last hour)
SELECT 
    s.sensor_id,
    s.location,
    COUNT(m.id) as actual_readings,
    3600 as expected_readings,
    ROUND((COUNT(m.id)::numeric / 3600 * 100), 2) as completeness_pct,
    CASE 
        WHEN COUNT(m.id)::float / 3600 >= 0.95 THEN 'Good'
        WHEN COUNT(m.id)::float / 3600 >= 0.80 THEN 'Warning'
        ELSE 'Poor'
    END as status
FROM sensors s
LEFT JOIN measurements m ON s.sensor_id = m.sensor_id
    AND m.timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY s.sensor_id, s.location;

-- 14. Out-of-range percentage
SELECT 
    sensor_id,
    COUNT(*) as total,
    SUM(CASE WHEN quality_flag = 'out_of_range' THEN 1 ELSE 0 END) as out_of_range,
    ROUND(
        SUM(CASE WHEN quality_flag = 'out_of_range' THEN 1 ELSE 0 END)::numeric / 
        COUNT(*)::numeric * 100, 
        2
    ) as out_of_range_pct
FROM measurements
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY sensor_id;

-- ============================================
-- PERFORMANCE QUERIES
-- ============================================

-- 15. Data ingestion rate
SELECT 
    DATE_TRUNC('minute', created_at) as minute,
    COUNT(*) as records_inserted,
    COUNT(*) / 60.0 as avg_per_second
FROM measurements
WHERE created_at >= NOW() - INTERVAL '10 minutes'
GROUP BY DATE_TRUNC('minute', created_at)
ORDER BY minute DESC;

-- 16. Database size statistics
SELECT 
    'measurements' as table_name,
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('measurements')) as total_size,
    pg_size_pretty(pg_relation_size('measurements')) as table_size,
    pg_size_pretty(pg_indexes_size('measurements')) as indexes_size
FROM measurements;

-- ============================================
-- UTILITY QUERIES
-- ============================================

-- 17. Export last hour data (for external analysis)
COPY (
    SELECT 
        m.sensor_id,
        s.location,
        s.sensor_type,
        m.timestamp,
        m.value,
        s.unit,
        m.quality_flag
    FROM measurements m
    JOIN sensors s ON m.sensor_id = s.sensor_id
    WHERE m.timestamp >= NOW() - INTERVAL '1 hour'
    ORDER BY m.timestamp, m.sensor_id
) TO '/tmp/sensor_data_export.csv' WITH CSV HEADER;

-- 18. Clean old data (older than 90 days)
DELETE FROM measurements 
WHERE timestamp < NOW() - INTERVAL '90 days';
VACUUM FULL measurements;