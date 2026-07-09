"""Provider interfaces for institutional ownership data."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class InstitutionalHoldingsProvider(ABC):
    """Interface for fetching raw 13F holdings."""

    @abstractmethod
    def fetch_13f(
        self,
        filing_date_gte: str | None = None,
        filing_date_lte: str | None = None,
        filer_cik: str | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame: ...


class FloatDataProvider(ABC):
    """Interface for fetching point-in-time float denominators."""

    @abstractmethod
    def fetch_float_data(
        self,
        tickers: list[str],
        periods: list[pd.Timestamp],
    ) -> pd.DataFrame: ...
