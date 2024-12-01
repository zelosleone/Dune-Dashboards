WITH active_traders AS (
    SELECT DISTINCT trader_id as address
    FROM (
        SELECT trader_id, amount_usd 
        FROM dex_solana.trades 
        WHERE block_month >= date_trunc('month', now() - interval '1' month)
        UNION ALL
        SELECT trader_id, amount_usd 
        FROM raydium_v4.trades
        WHERE block_month >= date_trunc('month', now() - interval '1' month)
        UNION ALL
        SELECT trader_id, amount_usd 
        FROM orca_whirlpool.trades
        WHERE block_month >= date_trunc('month', now() - interval '1' month)
    ) all_trades
    WHERE amount_usd > 10000
    LIMIT 100
),

date_series AS (
    SELECT DISTINCT block_month as month
    FROM dex_solana.trades
    WHERE block_month >= date_trunc('month', now() - interval '1' month)
),

dex_activity AS (
    SELECT 
        trader_id as whale_address,
        block_month as month,
        COUNT(*) as num_trades,
        SUM(CASE WHEN amount_usd > 0 THEN amount_usd ELSE 0 END) as total_volume_usd,
        SUM(CASE WHEN amount_usd > 0 THEN fee_usd ELSE 0 END) as total_fees_paid,
        AVG(amount_usd) as avg_trade_size
    FROM (
        SELECT * FROM dex_solana.trades
        UNION ALL 
        SELECT * FROM raydium_v4.trades
        UNION ALL
        SELECT * FROM orca_whirlpool.trades
    ) all_dex_trades
    WHERE trader_id IN (SELECT address FROM active_traders)
        AND block_month >= date_trunc('month', now() - interval '1' month)
    GROUP BY 1, 2
),

staking_activity AS (
  SELECT
    authority as whale_address,
    DATE_TRUNC('month', block_time) as month,
    COUNT(*) as num_stakes,
    SUM(stake) as total_staked,
    0 as fees,
    AVG(stake) as avg_stake_size
  FROM staking_solana.stake_actions
  WHERE authority IN (SELECT address FROM active_traders)
    AND action = 'delegate'
    AND block_time >= NOW() - interval '3' month
  GROUP BY 1, 2
),

transfer_activity AS (
  SELECT 
    from_owner as whale_address,
    DATE_TRUNC('month', block_time) as month,
    COUNT(*) as num_transfers,
    SUM(amount_usd) as total_transfer_volume,
    0 as fees,
    AVG(amount_usd) as avg_transfer_size
  FROM tokens_solana.transfers
  WHERE from_owner IN (SELECT address FROM active_traders)
    AND block_time >= NOW() - interval '3' month
  GROUP BY 1, 2
),

final_results AS (
  SELECT 
    w.address as whale_address,
    d.month,
    COALESCE(da.num_trades, 0) as num_trades,
    COALESCE(da.total_volume_usd, 0) as total_volume_usd,
    COALESCE(da.total_fees_paid, 0) as total_fees_paid,
    COALESCE(da.avg_trade_size, 0) as avg_trade_size,
    COALESCE(sa.num_stakes, 0) as num_stakes,
    COALESCE(sa.total_staked, 0) as total_staked,
    COALESCE(sa.avg_stake_size, 0) as avg_stake_size,
    COALESCE(ta.num_transfers, 0) as num_transfers,
    COALESCE(ta.total_transfer_volume, 0) as total_transfer_volume,
    COALESCE(ta.avg_transfer_size, 0) as avg_transfer_size
  FROM active_traders w
  CROSS JOIN date_series d
  LEFT JOIN dex_activity da ON da.whale_address = w.address AND da.month = d.month
  LEFT JOIN staking_activity sa ON sa.whale_address = w.address AND sa.month = d.month
  LEFT JOIN transfer_activity ta ON ta.whale_address = w.address AND ta.month = d.month
)

SELECT * 
FROM final_results
WHERE total_volume_usd > 0 
   OR total_staked > 0 
   OR total_transfer_volume > 0
ORDER BY total_volume_usd DESC
LIMIT 1000;