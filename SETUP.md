## Part 1: Part 1: Token Prices Schema Design

### Setting up the Tables

Database: `postgresql@14`

#### Strategy 1: Setup with psql + sql script

Create DB with psql + sql script

```commandline
psql -d postgres -f sql/create_database.sql
psql -d coingecko -f sql/ddl.sql
CREATE SCHEMA
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
```

#### (Optional): Strategy 2: Alembic as a database migration tool
- Alembic + SQLAlchemy is primarily meant for ingesting data with the ETL Pipeline
- It's okay to skip this

Create DB first

```commandline
psql -d postgres -f database/create_database.sql
CREATE DATABASE
```

Then, alembic upgrade

```commandline
alembic upgrade head

INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
INFO  [alembic.runtime.migration] Will assume transactional DDL.
INFO  [alembic.runtime.migration] Running upgrade  -> 68132ba866c9, Create token_mapping_table and prices
```

### ETL Pipeline to Ingest Tables

Provide credentials in `.env`

Note:
- The ETL Pipeline uses Async and is pretty fast
- With a free token, there will be rate limit errors

Sample Error

```txt
  Field required [type=missing, input_value={'status': {'error_code':...r higher rate limits."}}, input_type=dict]
```

```commandline
poetry shell
poetry install
export PYTHONPATH=.
python src/etl/etl_pipeline.py
```

### Querying Prices with DBT

Use default params in `dbt_project.yml`

```dbt_project.yml
vars:
  blockchain: "sui"
  contract_address: "0x549e8b69270defbfafd4f94e17ec44cdbdd99820b33bda2278dea3b9a32d3f55::cert::CERT"
  start_date: "2024-01-01 00:00:00"
  end_date: "2024-11-30 23:59:59"
  enrich_chain: "ethereum"
```

```commandline
dbt run --select query_prices
```
This creates a view: prices_schema.query_prices

Query it with

```sql
     coin_id     |    time_of_price    |        price         
-----------------+---------------------+----------------------
 volo-staked-sui | 2024-01-01 08:00:00 | 0.780157161890415400
 volo-staked-sui | 2024-01-02 08:00:00 | 0.847148697608270200
 volo-staked-sui | 2024-01-03 08:00:00 | 0.908168119707686200
 volo-staked-sui | 2024-01-04 08:00:00 | 0.843534307053116600
 volo-staked-sui | 2024-01-05 08:00:00 | 0.859337704685098000
```

Output of DBT:

```sql
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



## Part 2: Enriching erc20_token_transfer_table

Enriches the table with amount / raw_amount (USD prices of the given token_address)

The chain_id is `ethereum`

The token_address in erc20_token_transfer_table is the coin_id's contract address

Transcode to token_id with `prices_schema.coin_mapping_table`

Then query prices with 

```sql
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

In the example query below, the data is enriched into a view
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
