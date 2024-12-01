-- Get current whale wallets (>10000 SOL or >$1M in tokens)
WITH token_values AS (
  SELECT 
    lb.address,
    lb.sol_balance,
    lb.token_mint_address,
    lb.token_balance,
    lb.token_balance * COALESCE(p.price, 0) as token_value_usd
  FROM solana_utils.latest_balances lb
  LEFT JOIN dex_solana.price_hour p 
    ON lb.token_mint_address = p.contract_address
    AND p.hour = date_trunc('hour', CURRENT_TIMESTAMP)
),

whale_wallets AS (
  SELECT 
    address,
    sol_balance,
    SUM(token_value_usd) as total_token_value_usd
  FROM token_values
  GROUP BY 1, 2
  HAVING sol_balance > 10000 
    OR SUM(token_value_usd) > 1000000
),

-- Get their historical balance changes
balance_changes AS (
  SELECT
    address,
    date_trunc('day', day) as date,
    sol_balance,
    SUM(token_balance) as total_token_balance
  FROM solana_utils.daily_balances
  WHERE address IN (SELECT address FROM whale_wallets)
  GROUP BY 1, 2, 3
)

SELECT 
  date,
  address,
  sol_balance,
  total_token_balance,
  sol_balance + total_token_balance as total_balance
FROM balance_changes
ORDER BY date, total_balance DESC;