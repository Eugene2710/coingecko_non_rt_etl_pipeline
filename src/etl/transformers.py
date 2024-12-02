from datetime import datetime

from src.models.coingecko_models.data_models import (
    RawMapping,
    CoinMapping,
    RawPrices,
    Prices,
)


class MappingTransformer:
    """
    Transforms RawMapping from CoinGecko's coin list API into CoinMapping, before saving it in database
    """

    def transform(self, data: list[RawMapping]) -> list[CoinMapping]:
        all_coin_mapping: list[CoinMapping] = []
        for raw_mapping in data:
            for blockchain, contract_address in raw_mapping.platforms.items():
                all_coin_mapping.append(
                    CoinMapping(
                        blockchain=blockchain,
                        contract_address=contract_address,
                        coin_id=raw_mapping.id,
                        updated_at=datetime.utcnow(),
                    )
                )
        return all_coin_mapping


class HistoricalPriceTransformer:
    """
    Transforms RawPrices from CoinGecko's historical price by ID API into Prices, before saving it in database
    """

    def transform(self, data: RawPrices, coin_id: str) -> list[Prices]:
        historical_prices: list[Prices] = []
        for unix_timestamp, price in data.prices:
            historical_prices.append(
                Prices(
                    coin_id=coin_id,
                    time_of_price=datetime.fromtimestamp(unix_timestamp / 1000.0),
                    price=price,
                    fetched_at=datetime.utcnow(),
                )
            )
        return historical_prices
