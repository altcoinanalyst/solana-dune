-- the average transactions per second (TPS) of solana vs other competing chains 
WITH arbitrum_tps AS (
  SELECT
    DATE_TRUNC('day', block_time) AS time,
    'Arbitrum' AS blockchain,
    COUNT(*) / 60.0 / 60 / 24 AS tps
  FROM arbitrum.transactions
  WHERE
    block_time BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
  GROUP BY
    1
), Ethereum_TPS AS (
  SELECT
    DATE_TRUNC('day', block_time) AS time,
    'Ethereum' AS blockchain,
    COUNT(*) / 60.0 / 60 / 24 AS tps
  FROM ethereum.transactions
  WHERE
    block_time BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
  GROUP BY
    1
), solana_TPS AS (
  SELECT
    DATE_TRUNC('day', time) AS time,
    'Solana' AS blockchain,
    SUM(total_transactions) / (
      24 * 60 * 60
    ) AS tps
  FROM solana.blocks
  WHERE
    date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
  GROUP BY
    1
), base_TPS AS (
  SELECT
    DATE_TRUNC('day', block_time) AS time,
    'BASE' AS blockchain,
    COUNT(*) / 60.0 / 60 / 24 AS tps
  FROM base.transactions
  WHERE
    block_time BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
  GROUP BY
    1
)
SELECT
  *
FROM arbitrum_tps
UNION ALL
SELECT
  *
FROM Ethereum_TPS
UNION ALL
SELECT
  *
FROM solana_TPS
UNION ALL
SELECT
  *
FROM base_TPS
