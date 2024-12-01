WITH contract_stats AS (
    SELECT 
        c.name,
        c.namespace,
        COUNT(DISTINCT t."from") as unique_users,
        COUNT(*) as tx_count,
        SUM(CASE WHEN t.success = true THEN 1 ELSE 0 END) as successful_txs,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as share_of_total_txs
    FROM tron.transactions t
    LEFT JOIN tron.contracts c ON t."to" = c.address
    WHERE t.block_date >= CURRENT_DATE - interval '30' day
    GROUP BY c.name, c.namespace
    HAVING COUNT(*) > 1000
    ORDER BY tx_count DESC
    LIMIT 20
)
SELECT 
    COALESCE(name, 'Unknown Contract') as contract_name,
    COALESCE(namespace, 'Unknown') as protocol,
    unique_users,
    tx_count,
    successful_txs,
    share_of_total_txs as share_of_network_activity
FROM contract_stats;