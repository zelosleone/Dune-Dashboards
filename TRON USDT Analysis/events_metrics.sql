WITH date_spine AS (
    SELECT date_sequence as block_date 
    FROM unnest(sequence(date_add('day', -90, CURRENT_DATE), CURRENT_DATE, interval '1' day)) as t(date_sequence)
),
top_5_events AS (
    SELECT event_name
    FROM tron.logs_decoded
    WHERE block_date >= CURRENT_DATE - interval '90' day
    GROUP BY event_name
    ORDER BY COUNT(*) DESC
    LIMIT 5
),
daily_events AS (
    SELECT 
        block_date,
        event_name,
        COUNT(*) as event_count
    FROM tron.logs_decoded
    WHERE block_date >= CURRENT_DATE - interval '90' day
        AND event_name IN (SELECT event_name FROM top_5_events)
    GROUP BY block_date, event_name
)
SELECT 
    d.block_date,
    e.event_name,
    COALESCE(event_count, 0) as daily_events,
    ROUND(AVG(event_count) OVER (
        PARTITION BY e.event_name 
        ORDER BY d.block_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as events_7d_ma
FROM date_spine d
CROSS JOIN top_5_events e
LEFT JOIN daily_events de ON d.block_date = de.block_date 
    AND e.event_name = de.event_name
ORDER BY block_date, event_name;