WITH recent_trades AS (
    -- Get only recent high-value trades first
    SELECT 
        trader_id,
        SUM(amount_usd) as volume_24h
    FROM dex_solana.trades
    WHERE block_time >= NOW() - INTERVAL '24' hour
    AND amount_usd >= 1000  -- Focus on significant trades
    GROUP BY trader_id
    HAVING SUM(amount_usd) >= 10000  -- Only high volume traders
    LIMIT 100  -- Strict limit on initial dataset
),
active_whales AS (
    SELECT DISTINCT w.address
    FROM query_4357121 w
    INNER JOIN recent_trades r 
    ON w.address = r.trader_id
),
trading_summary AS (
    SELECT 
        t.trader_id,
        t.project,
        COUNT(*) as trades,
        SUM(amount_usd) as volume,
        MIN(block_time) as first_trade,
        MAX(block_time) as last_trade,
        COUNT(DISTINCT DATE_TRUNC('hour', block_time)) as active_hours
    FROM dex_solana.trades t
    WHERE t.trader_id IN (SELECT address FROM active_whales)
    AND t.block_time >= NOW() - INTERVAL '7' day  -- Last week only
    AND t.amount_usd > 0
    GROUP BY 1, 2
)
SELECT 
    trader_id as whale_address,
    project,
    trades as trade_count,
    ROUND(volume, 2) as total_volume_usd,
    active_hours,
    ROUND(volume/active_hours, 2) as avg_hourly_volume,
    first_trade,
    last_trade
FROM trading_summary
WHERE volume > 0
ORDER BY volume DESC
LIMIT 100