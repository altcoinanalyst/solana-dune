WITH sol_user_txns AS (
  SELECT
    block_date,
    COUNT(DISTINCT users) AS users
  FROM (
    SELECT
      block_date,
      signer AS users
    FROM solana.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
  ) AS combined_users
  GROUP BY
    block_date
), arb_user_txns AS (
  SELECT
    block_date,
    COUNT(DISTINCT users) AS users
  FROM (
    SELECT
      block_date,
      "from" AS users
    FROM arbitrum.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
    UNION ALL
    SELECT
      block_date,
      "to" AS users
    FROM arbitrum.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
  ) AS combined_users
  GROUP BY
    block_date
), bnb_user_txns AS (
  SELECT
    block_date,
    COUNT(DISTINCT users) AS users
  FROM (
    SELECT
      block_date,
      "from" AS users
    FROM bnb.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
    UNION ALL
    SELECT
      block_date,
      "to" AS users
    FROM bnb.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
  ) AS combined_users
  GROUP BY
    block_date
), eth_total_addrs AS (
  SELECT
    day,
    COUNT(DISTINCT addr) AS total_users
  FROM (
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      "from" AS addr
    FROM ethereum.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
    UNION ALL
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      to AS addr
    FROM ethereum.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
  ) AS a
  GROUP BY
    1
) , base_total_addrs as (
 SELECT
    block_date,
    COUNT(DISTINCT users) AS users
  FROM (
    SELECT
      block_date,
      "from" AS users
    FROM base.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
    UNION ALL
    SELECT
      block_date,
      "to" AS users
    FROM base.transactions
    WHERE
      block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
  ) AS combined_users
  GROUP BY
    block_date
)
SELECT
  'Ethereum' AS blockchain,
  AVG(tt.total_users) AS avg_users
FROM eth_total_addrs AS tt
GROUP BY
  1
UNION ALL
SELECT
  'Arbitrum' AS blockchain,
  AVG(tt.users) AS avg_users
FROM arb_user_txns AS tt
GROUP BY
  1
UNION ALL
SELECT
  'BSC' AS blockchain,
  AVG(tt.users) AS avg_users
FROM bnb_user_txns AS tt
GROUP BY
  1
UNION ALL
SELECT
  'Solana' AS blockchain,
  AVG(tt.users) AS avg_users
FROM sol_user_txns AS tt
GROUP BY
  1
  UNION ALL
SELECT
  'Base' AS blockchain,
  AVG(tt.users) AS avg_users
FROM base_total_addrs AS tt
GROUP BY
  1
