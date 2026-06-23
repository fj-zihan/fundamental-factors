"""
Massive REST API implementation of ShortInterestProvider.
https://massive.com/docs/rest/stocks/fundamentals/short-interest
"""

import pandas as pd
import requests

from .base import ShortInterestProvider


class MassiveProvider(ShortInterestProvider):

    BASE_URL = "https://api.massive.com"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self._headers = {"Authorization": f"Bearer {api_key}"}

    def fetch(
        self,
        tickers: list[str],
        settlement_date_gte: str,
        limit: int,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []

        for ticker in tickers:
            resp = requests.get(
                f"{self.BASE_URL}/stocks/v1/short-interest",
                headers=self._headers,
                params={
                    "ticker": ticker,
                    "settlement_date.gte": settlement_date_gte,
                    "limit": limit,
                    "sort": "settlement_date.desc",
                },
                timeout=30,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                frames.append(pd.DataFrame(results))

        if not frames:
            return pd.DataFrame(
                columns=["ticker", "settlement_date", "short_interest",
                         "avg_daily_volume", "days_to_cover"]
            )

        df = pd.concat(frames, ignore_index=True)
        df["settlement_date"] = pd.to_datetime(df["settlement_date"])
        return df
