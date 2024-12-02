-- Assumption
-- 1. One coin (e.g ethereum) will strictly have one unique coingecko coin_id. No change in mapping of chain + address to a coin_id over time
-- 2. chain is the name of the blockchain, as specified in coingecko. (refer to coins list api for a list of blockchain. e.g ethereum, polygon-pos)
-- 3. The user will always provide a valid, non-null chain and address. A valid one is one whose chain and address exist in coingecko

-- create a tokens table to store general info of the tokens
create table if not exists prices_schema.coin_mapping_table (
	blockchain text not null, -- e.g bitcoin
	contract_address text not null,
	coin_id text not null, -- a unique id on coingecko
	updated_at timestamp not null, -- the time at which the row of values was ingested
	primary key (blockchain, contract_address)
);
-- create a prices table where the coin prices can be queried by either api_id or contract_address, where both are unique
create table if not exists prices_schema.prices (
	coin_id text not null,
	time_of_price timestamp not null, -- time of price
	price numeric(38,18) not null,
	fetched_at timestamp not null default now(), -- time which these price was fetched from coingecko - different from time_of_price
	primary key (coin_id, time_of_price)
);

-- example of tokens
-- {
--    "id": "0chain",
--    "symbol": "zcn",
--    "name": "Zus",
--    "platforms": {
--      "ethereum": "0xb9ef770b6a5e12e45983c5d80545258aa38f3b78",
--      "polygon-pos": "0x8bb30e0e67b11b978a5040144c410e1ccddcba30"
--    }
--
-- {
--    "id": "usd-coin",
--    "symbol": "usdc",
--    "name": "USDC",
--    "platforms": {
--      "ethereum": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
--      "polkadot": "1337",
--      "zksync": "0x1d17cbcf0d6d143135ae902365d2e5e2a16538d4",
--      "optimistic-ethereum": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
--      "tron": "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8",
--      "stellar": "USDC-GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
--      "near-protocol": "17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1",
--      "hedera-hashgraph": "0.0.456858",
--      "base": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
--      "arbitrum-one": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
--      "polygon-pos": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
--      "sui": "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC",
--      "algorand": "31566704",
--      "avalanche": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
--      "solana": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
--      "celo": "0xceba9300f2b948710d2653dd7b07f33a8b32118c"
--    }

--Possible cases
--
-- input: ethereum, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
-- output - prices of USDC coin: type numeric
-- this input will be passed into the transcoded table to get the coin id
-- -> pass the coin id, from the transcoded table, into the get historical price by coin id API
-- ->

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

