
  create view "coingecko"."prices_schema"."enrich_tables__dbt_tmp"
    
    
  as (
    -- Strategy:
-- Transcode erc20_token_transfer_table's token_address + chain as ethereum into a coingecko token_id
-- Then, JOIN against the prices_schema.query_prices view from Part 1 to get prices
-- Get the price corresponding to the block time stamp from prices_schema.query_prices
-- query_prices has a resolution of 5 minutes. normalize the block_timestamp to 5 mins too
-- For simplicity, we made raw amount and amount the same for now.
-- raw amount seems to be the amount but in smallest denominations eg wei
SELECT
    t.transaction_hash,
    t.event_index,
    t.from_address,
    t.to_address,
    t.token_address,
    t.token_name,
    t.token_symbol,
    p.amount AS amount,
    p.amount AS raw_amount,
    t.block_timestamp,
    t.block_number,
    t.block_hash
FROM prices_schema.erc20_token_transfer_table t
JOIN prices_schema.coin_mapping_table cm
    ON cm.blockchain = 'ethereum' AND t.token_address = cm.contract_address
LEFT JOIN LATERAL (
    SELECT p.price as amount
    FROM prices_schema.query_prices p
    WHERE p.coin_id = cm.coin_id
      AND p.time_of_price = to_timestamp(floor(EXTRACT('epoch' FROM t.block_timestamp) / 300) * 300)
    ORDER BY p.time_of_price DESC
    LIMIT 1
) p ON TRUE
  );