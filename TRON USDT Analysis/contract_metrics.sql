WITH weekly_stats AS (
    SELECT 
        DATE_TRUNC('week', created_at) as week,
        COUNT(*) as new_contracts,
        COUNT(CASE WHEN dynamic = true THEN 1 END) as dynamic_contracts,
        LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('week', created_at)) as prev_week_contracts
    FROM tron.contracts
    WHERE created_at >= CURRENT_DATE - interval '180' day
    GROUP BY 1
),
namespace_stats AS (
    SELECT 
        namespace,
        COUNT(*) as contracts,
        ROUND(CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL), 2) as percentage
    FROM tron.contracts
    WHERE namespace IS NOT NULL 
      AND created_at >= CURRENT_DATE - interval '90' day
    GROUP BY namespace
    HAVING COUNT(*) > 5
)
SELECT 
    'weekly_trend' as metric_type,
    CAST(week AS VARCHAR) as time_key,
    new_contracts,
    dynamic_contracts,
    ROUND(CAST((new_contracts - prev_week_contracts) * 100.0 / NULLIF(prev_week_contracts, 0) AS DECIMAL), 2) as wow_growth,
    ROUND(CAST(dynamic_contracts * 100.0 / NULLIF(new_contracts, 0) AS DECIMAL), 2) as dynamic_percentage
FROM weekly_stats
UNION ALL
SELECT 
    'namespace_dist' as metric_type,
    namespace as time_key,
    contracts as new_contracts,
    NULL as dynamic_contracts,
    percentage as wow_growth,
    NULL as dynamic_percentage
FROM namespace_stats
ORDER BY metric_type, new_contracts DESC;