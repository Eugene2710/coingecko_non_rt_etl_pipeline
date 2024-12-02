from sqlalchemy import MetaData, Table, Column, String, DateTime, Numeric
from sqlalchemy.sql.functions import now

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

metadata: MetaData = MetaData()


"""
create table prices_schema.coin_mapping_table (
	blockchain text not null, -- e.g bitcoin
	contract_address text not null,
	coin_id text not null, -- a unique id on coingecko
	updated_at timestamp not null, -- the time at which the row of values was ingested
	primary key (blockchain, contract_address)
)
"""
coin_mapping_table = Table(
    "coin_mapping_table",
    metadata,
    Column(
        "blockchain", String, nullable=False, primary_key=True
    ),  # set to type String to support char indexing and length of id should not be long
    Column("contract_address", String, nullable=False, primary_key=True),
    Column("coin_id", String, nullable=False),
    Column("updated_at", DateTime, nullable=False, default=now()),
    schema="prices_schema",
)
"""
create table prices_schema.prices (
	coin_id text not null,
	time_of_price timestamp not null, -- time of price
	price numeric(38,18) not null,
	fetched_at timestamp not null default now(), -- time which these price was fetched from coingecko - different from time_of_price
	primary key (coin_id, time_of_price)
);
"""
prices_table = Table(
    "prices",
    metadata,
    Column("coin_id", String, nullable=False, primary_key=True),
    Column("time_of_price", DateTime, nullable=False, primary_key=True),
    Column("price", Numeric(precision=38, scale=18), nullable=False),
    Column("fetched_at", DateTime, nullable=False),
    schema="prices_schema",
)
