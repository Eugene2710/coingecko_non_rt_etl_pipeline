import os
from asyncio import Future
from datetime import datetime
import asyncio

from src.models.coingecko_models.data_models import (
    RawMapping,
    RawPrices,
    CoinMapping,
    Prices,
)
from src.etl.extractors import CoinMappingExtractor, HistoricalPriceExtractor
from src.etl.transformers import MappingTransformer, HistoricalPriceTransformer
from src.etl.batcher import SimpleBatcher
from src.etl.database_access_objects import CoinMappingDAO, PricesDAO

from dotenv import load_dotenv


class MappingETLPipeline:
    """
    Responsible for:
    1. Extracting data from Coingecko
    2. Saving data into table
    """

    def __init__(
        self,
        extractor: CoinMappingExtractor,
        transformer: MappingTransformer,
        batcher: SimpleBatcher[CoinMapping],
        loader: CoinMappingDAO,
    ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.batcher = batcher
        self.loader = loader

    async def run(self) -> None:
        raw_data: list[RawMapping] = await self.extractor.extract()
        transformed_data: list[CoinMapping] = self.transformer.transform(raw_data)
        # filter out mapped coins which do not have chain or contract address
        filtered_data: list[CoinMapping] = [
            coin for coin in transformed_data if coin.is_valid
        ]
        batches: list[list[CoinMapping]] = self.batcher.batch(filtered_data)
        futures: list[Future[None]] = [
            asyncio.ensure_future(self.loader.load(single_batch))
            for single_batch in batches
        ]
        combined_futures: Future[list[None]] = asyncio.gather(*futures)
        await combined_futures


class HistoricalPricesETLPipeline:
    def __init__(
        self,
        coin_mapping_dao: CoinMappingDAO,
        extractor: HistoricalPriceExtractor,
        transformer: HistoricalPriceTransformer,
        batcher: SimpleBatcher[Prices],
        loader: PricesDAO,
    ) -> None:
        self.coin_mapping_dao = coin_mapping_dao
        self.extractor = extractor
        self.transformer = transformer
        self.batcher = batcher
        self.loader = loader

    async def extract_and_transform(
        self, coin_id: str, start_date: datetime, end_date: datetime
    ) -> list[Prices]:
        raw_price: RawPrices | None = await self.extractor.extract(
            coin_id=coin_id, start_date=start_date, end_date=end_date
        )
        transformed_data: list[Prices] = (
            self.transformer.transform(raw_price, coin_id) if raw_price else []
        )
        return transformed_data

    async def run(self) -> None:
        """
        note:
        - free api only supports prices for current year
        - free api tier: 30 API calls/minute, paid tier: 500 API calls/minute
        """
        sample_token: int = 10
        start: int = 60
        end: int = start + sample_token
        start_date: datetime = datetime(year=2024, month=1, day=1)
        end_date: datetime = datetime(year=2024, month=11, day=30)
        coin_ids: list[str] = await self.coin_mapping_dao.fetch_distinct_coin_ids()
        # default coin ids are used to curb rate limit for free api tier
        default_coin_ids = [
            "ape-lol",
            "solana",
            "tron",
            "binance-smart-chain",
            "ethereum",
            "arbitrum-one",
        ]
        coin_ids = default_coin_ids + coin_ids
        coin_ids = coin_ids[start:end]
        futures: list[Future[list[Prices]]] = [
            asyncio.ensure_future(
                self.extract_and_transform(
                    coin_id=coin_id, start_date=start_date, end_date=end_date
                )
            )
            for coin_id in coin_ids
        ]
        all_future: Future[list[list[Prices]]] = asyncio.gather(*futures)
        transformed_data: list[list[Prices]] = await all_future
        flattened_data: list[Prices] = [
            item for sublist in transformed_data for item in sublist
        ]
        batches: list[list[Prices]] = self.batcher.batch(flattened_data)

        loader_futures: list[Future[None]] = [
            asyncio.ensure_future(self.loader.load(single_batch))
            for single_batch in batches
        ]
        combined_future: Future[list[None]] = asyncio.gather(*loader_futures)
        await combined_future


if __name__ == "__main__":
    load_dotenv()
    event_loop = asyncio.new_event_loop()
    coin_mapping_dao = CoinMappingDAO(os.getenv("PG_CONNECTION_STRING", ""))
    mapping_etl_pipeline: MappingETLPipeline = MappingETLPipeline(
        CoinMappingExtractor(), MappingTransformer(), SimpleBatcher(), coin_mapping_dao
    )
    historical_prices_etl_pipeline: HistoricalPricesETLPipeline = (
        HistoricalPricesETLPipeline(
            coin_mapping_dao,
            HistoricalPriceExtractor(),
            HistoricalPriceTransformer(),
            SimpleBatcher(),
            PricesDAO(os.getenv("PG_CONNECTION_STRING", "")),
        )
    )
    event_loop.run_until_complete(mapping_etl_pipeline.run())
    event_loop.run_until_complete(historical_prices_etl_pipeline.run())
