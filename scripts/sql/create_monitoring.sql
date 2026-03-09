-- =====================================================
-- 1. Таблица для мониторинга качества данных
-- =====================================================
CREATE TABLE IF NOT EXISTS pikcha_mart.data_quality_metrics (
    check_date Date DEFAULT today(),
    table_name String,
    total_records UInt32,
    duplicate_count UInt32,
    duplicate_percentage Float32,
    null_count UInt32,
    empty_string_count UInt32,
    invalid_date_count UInt32,
    quality_score Float32,
    alert_level String DEFAULT 'OK',
    checked_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (check_date, table_name);

-- =====================================================
-- 2. MV для мониторинга дубликатов в покупках
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS pikcha_raw.duplicate_monitor_mv
TO pikcha_mart.data_quality_metrics
AS
SELECT
    today() AS check_date,
    'purchases_raw' AS table_name,
    count() AS total_records,
    count() - countDistinct(JSONExtractString(data, 'purchase_id')) AS duplicate_count,
    round((count() - countDistinct(JSONExtractString(data, 'purchase_id'))) * 100.0 / count(), 2) AS duplicate_percentage,
    countIf(JSONExtractString(data, 'purchase_id') = '') AS null_count,
    countIf(length(trimBoth(JSONExtractString(data, 'purchase_id'))) = 0) AS empty_string_count,
    countIf(parseDateTimeBestEffortOrNull(JSONExtractString(data, 'purchase_datetime')) > now()) AS invalid_date_count,
    round(100 - (
        ((count() - countDistinct(JSONExtractString(data, 'purchase_id'))) * 100.0 / count()) +
        (countIf(JSONExtractString(data, 'purchase_id') = '') * 100.0 / count()) +
        (countIf(parseDateTimeBestEffortOrNull(JSONExtractString(data, 'purchase_datetime')) > now()) * 100.0 / count())
    ), 2) AS quality_score,
    multiIf(
        duplicate_percentage > 50, 'CRITICAL',
        duplicate_percentage > 30, 'WARNING',
        duplicate_percentage > 10, 'INFO',
        'OK'
    ) AS alert_level,
    now() AS checked_at
FROM pikcha_raw.purchases_raw;

-- =====================================================
-- 3. Представление для алертов (удобно для Grafana)
-- =====================================================
CREATE VIEW IF NOT EXISTS pikcha_mart.quality_alerts AS
SELECT
    check_date,
    table_name,
    duplicate_percentage,
    alert_level,
    quality_score,
    checked_at
FROM pikcha_mart.data_quality_metrics
WHERE alert_level IN ('CRITICAL', 'WARNING')
ORDER BY check_date DESC, duplicate_percentage DESC;