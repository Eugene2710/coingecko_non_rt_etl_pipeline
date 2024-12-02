from typing import Generic

from src.models.coingecko_models.data_models import TransformedData


class SimpleBatcher(Generic[TransformedData]):
    """
    Responsible for batching the list of transformed data into a nested list of transformed data
    - current batch size is set at 1000
    """

    def __init__(self, batch_size: int = 1000):
        self.batch_size: int = batch_size

    def batch(self, input: list[TransformedData]) -> list[list[TransformedData]]:
        return [
            input[i : i + self.batch_size]
            for i in range(0, len(input), self.batch_size)
        ]
