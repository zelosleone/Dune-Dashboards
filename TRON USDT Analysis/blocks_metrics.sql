WITH date_spine AS (
    SELECT date_sequence as date 
    FROM unnest(sequence(date_add('day', -90, CURRENT_DATE), CURRENT_DATE, interval '1' day)) as t(date_sequence)
),
daily_metrics AS (
    SELECT 
        date,
        COUNT(*) as blocks_count,
        AVG(gas_used) as avg_gas_used,
        AVG(size) as avg_block_size
    FROM tron.blocks
    WHERE date >= CURRENT_DATE - interval '90' day
    GROUP BY date
)
SELECT 
    d.date,
    COALESCE(blocks_count, 0) as blocks,
    COALESCE(avg_gas_used, 0) as avg_gas,
    COALESCE(avg_block_size, 0) as avg_size,
    ROUND(AVG(blocks_count) OVER (
        ORDER BY d.date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as blocks_7d_ma
FROM date_spine d
LEFT JOIN daily_metrics m ON d.date = m.date
ORDER BY date;