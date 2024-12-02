from typing import TypeVar
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel

RawData = TypeVar("RawData", bound=BaseModel)
TransformedData = TypeVar("TransformedData", bound=BaseModel)


class RawMapping(BaseModel):
    """
    Raw Data Model for response from coin list api
    Docs: https://docs.coingecko.com/v3.0.1/reference/coins-list
    Example:
    {
    "id": "usd-coin",
    "symbol": "usdc",
    "name": "USDC",
    "platforms": {
      "ethereum": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
      "polkadot": "1337",
      "zksync": "0x1d17cbcf0d6d143135ae902365d2e5e2a16538d4",
      "optimistic-ethereum": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
      "tron": "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8",
      "stellar": "USDC-GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
      "near-protocol": "17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1",
      "hedera-hashgraph": "0.0.456858",
      "base": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
      "arbitrum-one": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
      "polygon-pos": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
      "sui": "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC",
      "algorand": "31566704",
      "avalanche": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
      "solana": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
      "celo": "0xceba9300f2b948710d2653dd7b07f33a8b32118c"
    }
    """

    id: str
    symbol: str
    name: str
    platforms: dict[str, str]


class CoinMapping(BaseModel):
    """
    Transformed data from RawMapping
    """

    blockchain: str
    contract_address: str
    coin_id: str
    updated_at: datetime

    @property
    def is_valid(self) -> bool:
        """
        return only when ALL 3 fields are not empty
        """
        return (
            self.blockchain.strip() != ""
            and self.contract_address.strip() != ""
            and self.coin_id.strip() != ""
        )


class RawPrices(BaseModel):
    """
      Raw Data Model for response from historical prices api, where only the prices are extracted
      - market cap and volume are ignored since they are not required in the problem statement
      Docs: https://docs.coingecko.com/v3.0.1/reference/coins-id-market-chart
      Example:
      {
    "prices": [
      [
        1732956062478,
        3697.584835543712
      ],
      [
        1732956317891,
        3697.3869553432323
      ],
      [
        1732956710303,
        3696.9658998654777
      ],
      }
    """

    prices: list[tuple[int, Decimal]]


class Prices(BaseModel):
    """
    Transformed data model from RawPrices
    note: float data type cannot be used because float data type is of 64 bits which translates to a 15 to 16 dp of precision
    this would be insufficient decimal points hence, decimal data type is used instead
    """

    coin_id: str
    time_of_price: datetime
    price: Decimal
    fetched_at: datetime
