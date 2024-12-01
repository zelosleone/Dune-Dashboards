WITH whale_wallets AS (
  SELECT *
  FROM query_4357121
),
filtered_transfers AS (
  -- Outgoing transfers from whales
  SELECT 
    from_owner as whale_address,
    to_owner as counterparty,
    amount_usd,
    block_time,
    token_mint_address
  FROM tokens_solana.transfers
  WHERE from_owner IN (SELECT address FROM whale_wallets)
    AND block_time >= NOW() - INTERVAL '30' day
    AND amount_usd >= 1000
  
  UNION ALL
  
  -- Incoming transfers to whales  
  SELECT 
    to_owner as whale_address,
    from_owner as counterparty,
    amount_usd,
    block_time,
    token_mint_address
  FROM tokens_solana.transfers  
  WHERE to_owner IN (SELECT address FROM whale_wallets)
    AND block_time >= NOW() - INTERVAL '30' day
    AND amount_usd >= 1000
)

SELECT 
  whale_address,
  counterparty,
  COUNT(*) as interaction_count,
  SUM(amount_usd) as total_volume_usd,
  COUNT(DISTINCT DATE_TRUNC('day', block_time)) as days_active,
  COUNT(DISTINCT token_mint_address) as unique_tokens_transferred
FROM filtered_transfers
GROUP BY 1, 2
HAVING COUNT(*) > 5
ORDER BY total_volume_usd DESC
LIMIT 1000;