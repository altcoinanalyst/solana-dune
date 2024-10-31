with
    evm_traders as (
        SELECT 
            date_trunc('month',block_time) as month
            , blockchain
            , COUNT(DISTINCT trader) AS traders
        FROM nft.trades
        CROSS JOIN UNNEST(ARRAY[buyer, seller]) AS t(trader)
        WHERE blockchain != 'solana'
        and block_time  BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH AND CURRENT_DATE
        group by 1,2
    )

    , solana_traders as (
        SELECT 
            date_trunc('month',block_time) as month
            , blockchain
            , COUNT(DISTINCT trader) AS traders
        FROM nft_solana.trades
        CROSS JOIN UNNEST(ARRAY[buyer, seller]) AS t(trader)
        WHERE block_time  BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12' MONTH AND CURRENT_DATE
        group by 1,2
    )

SELECT 
    t.*
    , e.traders
FROM (
    SELECT 
        cast(date_trunc('month', block_time) as timestamp) month
        , tr.blockchain
        , sum(amount_usd) as volume_usd
    FROM nft.trades tr
    WHERE block_time  BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12' MONTH AND CURRENT_DATE
    and tr.blockchain != 'solana'
    group by 1,2
) t
LEFT JOIN evm_traders e ON e.month = t.month and e.blockchain = t.blockchain

UNION ALL 

SELECT 
t.*
, s.traders
FROM (
    SELECT 
        cast(date_trunc('month', block_time) as timestamp) month
        , tr.blockchain
        , sum(amount_usd) as volume_usd
    FROM nft_solana.trades tr
    LEFT JOIN tokens_solana.nft tk ON tr.account_mint = tk.account_mint
    WHERE block_time   BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12' MONTH AND CURRENT_DATE
    group by 1,2
) t 
LEFT JOIN solana_traders s ON s.month = t.month and s.blockchain = t.blockchain
