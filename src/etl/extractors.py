import logging

import os
import pydantic
from typing import Any
from datetime import datetime

from dotenv import load_dotenv
from src.models.coingecko_models.data_models import RawMapping, RawPrices
from src.exceptions.coingecko_client_error import CoinGeckoClientError
from retry import retry
from urllib.parse import quote, urlencode

import asyncio
import aiohttp

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()],
)


class CoinMappingExtractor:
    """
    Responsible for querying Coingecko for coin list mapping api
    input: blockchain + contrat_address
    output: coingecko coin_id
    """

    @retry(
        exceptions=(aiohttp.ClientError, CoinGeckoClientError),
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def extract(self) -> list[RawMapping]:
        url: str = (
            f"https://api.coingecko.com/api/v3/coins/list?include_platform=true&x_cg_demo_api_key={quote(os.getenv('COIN_GECKO_API_KEY', ''))}"
        )
        headers: dict[str, str] = {
            "accept": "application/json",
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, ssl=False) as response:
                if response.status == 200:
                    response_dict: list[dict[str, Any]] = await response.json()
                    deserialised_response: list[RawMapping] = [
                        RawMapping.model_validate(raw_dict) for raw_dict in response_dict
                    ]
                else:
                    # can happen when coingecko server is down
                    raise CoinGeckoClientError(
                        f"Received non-status code 200: {response.status}"
                    )
        return deserialised_response


class HistoricalPriceExtractor:
    """
    Responsible for querying Coingecko for historical prices given an coin_id for a given range
    Precision of price: 18 dp
    Hence, Decimal(precision=36, scale=18) is used
    https://docs.coingecko.com/v3.0.1/reference/coins-id-market-chart-range
    """

    async def extract(
        self, coin_id: str, start_date: datetime, end_date: datetime
    ) -> RawPrices | None:
        base_url: str = (
            f"https://api.coingecko.com/api/v3/coins/{quote(coin_id)}/market_chart/range"
        )
        params: dict[str, Any] = {
            "vs_currency": "usd",
            "from": int(start_date.timestamp()),
            "to": int(end_date.timestamp()),
            "precision": 18,
            "x_cg_demo_api_key": os.getenv("COIN_GECKO_API_KEY"),
        }
        url: str = f"{base_url}?{urlencode(params)}"
        headers: dict[str, str] = {
            "accept": "application/json",
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url=url, headers=headers, ssl=False) as resp:
                response: list[str] = await resp.json()
                try:
                    raw_prices: RawPrices | None = RawPrices.model_validate(response)
                except pydantic.ValidationError:
                    # coin is not found
                    raw_prices = None
        return raw_prices


if __name__ == "__main__":
    load_dotenv()
    event_loop = asyncio.new_event_loop()

    # coinMap: CoinMappingExtractor = CoinMappingExtractor()
    # response = event_loop.run_until_complete(coinMap.extract())
    # print(response)

    prices_extractor: HistoricalPriceExtractor = HistoricalPriceExtractor()
    response = event_loop.run_until_complete(
        prices_extractor.extract(
            coin_id="brn-metaverse",
            start_date=datetime(year=2024, month=10, day=1),
            end_date=datetime(year=2024, month=10, day=2),
        )
    )
    print(response)
