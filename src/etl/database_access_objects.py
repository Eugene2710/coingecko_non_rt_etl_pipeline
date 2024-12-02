import logging
from abc import ABC
from typing import Generic, Any
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, CursorResult
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from database_management.tables import coin_mapping_table, prices_table
from src.models.coingecko_models.data_models import TransformedData, CoinMapping, Prices

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s = %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()],
)


class AbstractDAO(ABC, Generic[TransformedData]):
    """
    Responsible for loading a batch of data models into the database
    Abstract DAO created so that async engine does not need to be created in each DAO service,
    and instill a need to take care of exceptions
    """

    def __init__(self, connection_string: str):
        self._engine = create_async_engine(connection_string)

    async def load(self, batch: list[TransformedData]) -> None:
        raise NotImplementedError()


class CoinMappingDAO(AbstractDAO[CoinMapping]):
    """
    load function: loads the values from list of data into coin_mapping_table
    fetch_distinct_coin_ids function: fetches a list of distinct coin_ids from the coin_mapping_table
    """

    async def load(self, batch: list[TransformedData]) -> None:
        async with self._engine.begin() as conn:
            serialized_batch: list[dict[str, Any]] = [
                single_item.model_dump() for single_item in batch
            ]
            try:
                smt = (
                    insert(coin_mapping_table)
                    .values(serialized_batch)
                    .on_conflict_do_nothing()
                )
                await conn.execute(smt)
            except SQLAlchemyError as e:
                logging.exception(e)
                raise e

    async def fetch_distinct_coin_ids(self) -> list[str]:
        async with self._engine.begin() as conn:
            result: CursorResult[tuple[str]] = await conn.execute(
                select(coin_mapping_table.c.coin_id).distinct()
            )
            distinct_ids = [row[0] for row in result.fetchall()]
        return distinct_ids


class PricesDAO(AbstractDAO[Prices]):
    """
    load function: loads list of Price into prices_table
    """

    async def load(self, batch: list[Prices]) -> None:
        async with self._engine.begin() as conn:
            serialized_batch: list[dict[str, Any]] = [
                single_item.model_dump() for single_item in batch
            ]
            try:
                smt = (
                    insert(prices_table)
                    .values(serialized_batch)
                    .on_conflict_do_nothing()
                )
                await conn.execute(smt)
            except SQLAlchemyError as e:
                logging.exception(e)
                raise e
