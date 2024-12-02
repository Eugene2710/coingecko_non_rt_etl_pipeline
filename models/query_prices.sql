WITH input_param AS (
    SELECT
        '{{ var('blockchain') }}'::TEXT AS blockchain,
        '{{ var('contract_address') }}'::TEXT AS contract_address,
        '{{ var('start_date') }}'::TIMESTAMP AS start_date,
        '{{ var('end_date') }}'::TIMESTAMP AS end_date
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