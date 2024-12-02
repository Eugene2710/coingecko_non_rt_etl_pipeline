## Part 1: Token Prices Schema Design

#### Requirements

1. Develop and provide the Data Definition Language for required tables
2. Historical token price data (USD) of a given chain and token address can be fetched with a simple query
3. Take into account how the same token (E.G USDC) can have different contract addresses across different Blockchains
4. (Bonus) Describe and implement the logic needed for ingesting historical price data from CoinGecko's APIs

#### Strategy
1. Create a mapping table that maps a blockchain and contract address to q unique coingecko id/coin_id
2. Price table will store historical prices for a given coin_id. To get price given inputs, transcode the inputs into the coingecko id, then get prices from the prices table.

```python3
def get_historical_price(blockchain: str, contract_address: str) -> float:
    pass
```
Solution: 
- Avoid JOINs, as much as possible. Keep the structure of the historical price table flat

#### Assumptions
1. One coin (e.g ethereum) will strictly have one unique coingecko coin_id. No change in mapping of blockchain + contract address to a coin_id over time
2. Blockchain is the name of the blockchain, as specified in coingecko. (refer to coins list api for a list of blockchain. e.g ethereum, polygon-pos)
3. The user will always provide a valid, non-null blockchain and contract_address. A valid one is one whose blockchain and contract address exist in coingecko

#### Proposed Strategy
DDL Tables

1. Transcoding Table

Responsibility: Maps a contract address and blockchain to a coin_id
This coin_id will then be used with the prices table to get the price

```sql
CREATE DATABASE coingecko;

\c coingecko;

CREATE SCHEMA prices_schema;

-- PK constraint on chain + token address
-- A token_id can have multiple chain + token address mappings
create table if not exists prices_schema.coin_mapping_table (
	blockchain text not null, -- e.g bitcoin
	contract_address text not null,
	coin_id text not null, -- a unique id on coingecko
	updated_at timestamp not null, -- the time at which the row of values was ingested
	primary key (blockchain, contract_address)
);
```

API used:
2. Prices Table

Responsibility: Stores the time-series prices of a given coin_id

Resolution is limited by CoinGecko API

```SQL
create table if not exists prices_schema.prices (
	coin_id text not null,
	time_of_price timestamp not null, -- time of price
	price numeric(38,18) not null,
	fetched_at timestamp not null default now(), -- time which these price was fetched from coingecko - different from time_of_price
	primary key (coin_id, time_of_price)
);
```

#### Query to get price

Strategy, with a CT

```SQL
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
```

## Part 2: Enriching erc20_token_transfer_table

The chain_id is `ethereum`

The token_address in erc20_token_transfer_table is the token_id's token address

Transcode to coin_id with `prices_schema.coin_mapping_table`

Then query prices with 

```SQL
-- Represents table before enriching
CREATE TABLE IF NOT EXISTS prices_schema.erc20_token_transfer_table(
    transaction_hash TEXT NOT NULL, -- Blockchain Transactions Can Have Multiple Events: On Ethereum and other EVM-compatible blockchains, a single transaction can trigger multiple events, including multiple ERC20 token transfers.
    event_index INTEGER NOT NULL,   -- Integer that represents the position of the event within the transaction's list of logs.
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    token_name TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    PRIMARY KEY (transaction_hash, event_index)
);

-- Represents table after enriching
-- Not used, but to show how the final structure will look like
-- In the dbt script `enrich_table.sql`, we create a view to show the results instead
-- That result can be inserted into this
CREATE TABLE IF NOT EXISTS prices_schema.erc20_token_transfer_table_enriched(
    transaction_hash TEXT NOT NULL, -- Blockchain Transactions Can Have Multiple Events: On Ethereum and other EVM-compatible blockchains, a single transaction can trigger multiple events, including multiple ERC20 token transfers.
    event_index INTEGER NOT NULL,   -- Integer that represents the position of the event within the transaction's list of logs.
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    token_name TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    raw_amount DECIMAL(38, 18) NOT NULL,
    amount DECIMAL(38, 18) NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    PRIMARY KEY (transaction_hash, event_index)
);
```

In the example query below, we enrich the data into a view
- Due to time constraint, I could not enrich the erc20_token_transfer_table
- But the syntax is confirmed to be right with a test run

```commandline
dbt run --select enrich_tables
15:13:30  Running with dbt=1.8.9
15:13:30  Registered adapter: postgres=1.8.2
15:13:31  Found 2 models, 428 macros
15:13:31  
15:13:31  Concurrency: 1 threads (target='dev')
15:13:31  
15:13:31  1 of 1 START sql view model prices_schema.enrich_tables ........................ [RUN]
15:13:31  1 of 1 OK created sql view model prices_schema.enrich_tables ................... [CREATE VIEW in 0.06s]
15:13:31  
15:13:31  Finished running 1 view model in 0 hours 0 minutes and 0.23 seconds (0.23s).
15:13:31  
15:13:31  Completed successfully
15:13:31  
15:13:31  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

Query

```
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
```

## CSV outputs
The outputs have also been stored in the csv_outputs folder to show the expected outputs.