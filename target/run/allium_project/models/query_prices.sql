
  create view "coingecko"."prices_schema"."query_prices__dbt_tmp"
    
    
  as (
    WITH input_param AS (
    SELECT
        'sui'::TEXT AS blockchain,
        '0x549e8b69270defbfafd4f94e17ec44cdbdd99820b33bda2278dea3b9a32d3f55::cert::CERT'::TEXT AS contract_address,
        '2024-01-01 00:00:00'::TIMESTAMP AS start_date,
        '2024-11-30 23:59:59'::TIMESTAMP AS end_date
),
transcoded_coin_id AS (
    SELECT coin_id
    FROM prices_schema.coin_mapping_table c
    JOIN input_param i ON c.blockchain = i.blockchain AND c.contract_address = i.contract_address
)
SELECT
    p.coin_id,
    date_trunc('hour', p.time_of_price) + INTERVAL '5 minutes' * FLOOR(EXTRACT(MINUTE FROM p.time_of_price)::INT / 5) AS time_of_price,
    p.price
FROM prices_schema.prices p JOIN transcoded_coin_id t ON p.coin_id = t.coin_id
JOIN input_param i ON TRUE
WHERE p.time_of_price BETWEEN i.start_date AND i.end_date
ORDER BY time_of_price
  );