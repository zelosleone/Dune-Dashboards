WITH date_spine AS (
    SELECT date_sequence as block_date 
    FROM unnest(sequence(date_add('day', -90, CURRENT_DATE), CURRENT_DATE, interval '1' day)) as t(date_sequence)
),
daily_stats AS (
    SELECT 
        block_date,
        COUNT(*) as total_transactions,
        COUNT(CASE WHEN success = true THEN 1 END) as successful_txs,
        AVG(gas_used) as avg_gas
    FROM tron.transactions t
    WHERE block_date >= CURRENT_DATE - interval '90' day
    GROUP BY block_date
)
SELECT 
    d.block_date,
    COALESCE(total_transactions, 0) as transactions,
    COALESCE(successful_txs, 0) as successful_txs,
    ROUND(AVG(total_transactions) OVER (ORDER BY d.block_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) as transactions_7d_ma,
    COALESCE(avg_gas, 0) as avg_gas_used
FROM date_spine d
LEFT JOIN daily_stats s ON d.block_date = s.block_date
ORDER BY block_date;