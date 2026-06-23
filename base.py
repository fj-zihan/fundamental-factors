"""
Abstract base class for short interest data providers.
Implement fetch() to add a new data source without touching asset code.
"""

from abc import ABC, abstractmethod
import pandas as pd


class ShortInterestProvider(ABC):
    """
    Interface for fetching short interest data.

    Output schema
    -------------
    ticker            : str
    settlement_date   : datetime
    short_interest    : int
    avg_daily_volume  : int
    days_to_cover     : float
    """

    @abstractmethod
    def fetch(
        self,
        tickers: list[str],
        settlement_date_gte: str,
        limit: int,
    ) -> pd.DataFrame: ...
