-- ============================================================
-- transformations/customer_events_clean.sql
-- Transform: raw customer_events - customer_events_clean
-- Reads from: raw_customer_events (written by the ingestor)
-- Writes to: customer_events_clean (analytics-ready layer)
-- executed automatically after every load
-- ============================================================


-- Drop and recreate so the clean table always reflects latest raw data
DROP TABLE IF EXISTS customer_events_clean;

CREATE TABLE customer_events_clean AS
SELECT
    -- Identity
    event_id,
    user_id,

    -- Timestamp: cast to TIMESTAMP type for consistent sorting/filtering
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,

    -- Event type: trim whitespace and lowercase for consistency
    LOWER(TRIM(event_type)) AS event_type,

    -- Only keep rows that have the three mandatory columns
    -- (rows with nulls in these columns are excluded)

    -- Audit column added by the transform
    CURRENT_TIMESTAMP AS processed_at

FROM raw_customer_events

WHERE
    event_id        IS NOT NULL
    AND user_id     IS NOT NULL
    AND event_timestamp IS NOT NULL
    AND user_id     > 0;