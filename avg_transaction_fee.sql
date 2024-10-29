WITH arb_transaction AS (
    SELECT
        DATE_TRUNC('day', block_time) AS dt,
        AVG(transaction_fee) AS arb_transaction_fees
    FROM (
        SELECT
            *,
            (CAST(gas_used AS DOUBLE) + CAST(gas_used_for_l1 AS DOUBLE)) * CAST(effective_gas_price AS DOUBLE) / 10e17 AS transaction_fee
        FROM arbitrum.transactions
        WHERE block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
    )
    GROUP BY 1
), base_chain AS (
    SELECT
        DATE_TRUNC('day', block_time) AS dt,
        AVG(transaction_fee) AS base_transaction_fees
    FROM (
        SELECT
            *,
            ((CAST(gas_used AS DOUBLE) * CAST(gas_price AS DOUBLE)) + CAST(l1_fee AS DOUBLE)) / 1e18 AS transaction_fee
        FROM base.transactions
        WHERE block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
    )
    GROUP BY 1
), ethereum AS (
    SELECT
        DATE_TRUNC('day', block_time) AS dt,
        AVG(transaction_fee) AS eth_transaction_fees
    FROM (
        SELECT
            *,
            CASE
                WHEN (type = 'Legacy' OR type = 'AccessList') THEN CAST(gas_price AS DOUBLE) * CAST(t.gas_used AS DOUBLE) / 1e18
                WHEN type = 'DynamicFee' AND base_fee_per_gas + max_priority_fee_per_gas <= max_fee_per_gas THEN (CAST(base_fee_per_gas AS DOUBLE) + COALESCE(CAST(max_priority_fee_per_gas AS DOUBLE), 0)) * CAST(t.gas_used AS DOUBLE) / 1e18
                WHEN type = 'DynamicFee' AND base_fee_per_gas + max_priority_fee_per_gas > max_fee_per_gas THEN (CAST(max_fee_per_gas AS DOUBLE) * CAST(t.gas_used AS DOUBLE)) / 1e18
            END AS transaction_fee
        FROM ethereum.transactions AS t
        LEFT JOIN ethereum.blocks AS b ON block_number = number
        WHERE block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
    )
    GROUP BY 1
), eth_price AS (
    SELECT
        DATE_TRUNC('day', minute) AS dt,
        AVG(price) AS eth_price
    FROM prices.usd
    WHERE symbol = 'WETH'
    AND minute BETWEEN DATE_TRUNC('day', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
    GROUP BY 1
), solana AS (
    SELECT
        DATE_TRUNC('day', block_time) AS dt,
        AVG(fee / 1e9) AS avg_fee_sol
    FROM solana.transactions
    WHERE block_date BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
    GROUP BY 1
), solprice AS (
    SELECT
        DATE_TRUNC('day', minute) AS day,
        AVG(price) AS price
    FROM prices.usd
    WHERE contract_address IS NULL
    AND symbol = 'SOL'
    AND minute BETWEEN DATE_TRUNC('day', CURRENT_DATE) - INTERVAL '1' MONTH AND CURRENT_DATE
    AND blockchain IS NULL
    GROUP BY 1
), basetable AS (
    SELECT
        a.dt,
        a.arb_transaction_fees * ep.eth_price AS arb_fee_usd,
        e.eth_transaction_fees * ep.eth_price AS eth_fee_usd,
        s.avg_fee_sol * sp.price AS sol_fee_usd,
        bc.base_transaction_fees * ep.eth_price AS base_fee_usd
    FROM arb_transaction AS a
    JOIN ethereum e ON e.dt = a.dt
    JOIN eth_price AS ep ON ep.dt = a.dt
    JOIN solana AS s ON s.dt = a.dt
    JOIN solprice AS sp ON a.dt = sp.day
    JOIN base_chain AS bc ON a.dt = bc.dt
), pivot_table AS (
    SELECT
        'Arbitrum' AS blockchain, AVG(arb_fee_usd) AS average_fee
    FROM basetable
    UNION ALL
    SELECT
        'Ethereum', AVG(eth_fee_usd)
    FROM basetable
    UNION ALL
    SELECT
        'Solana', AVG(sol_fee_usd)
    FROM basetable
    UNION ALL
    SELECT
        'Base', AVG(base_fee_usd)
    FROM basetable
)
SELECT blockchain, average_fee
FROM pivot_table
order BY average_fee desc;
