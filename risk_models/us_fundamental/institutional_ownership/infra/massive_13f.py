"""Massive REST API implementation for 13F filings."""

from __future__ import annotations

import pandas as pd
import requests

from ..config import MASSIVE_13F_PATH
from .base import InstitutionalHoldingsProvider


RAW_13F_COLUMNS = [
    "accession_number",
    "cusip",
    "file_number",
    "filer_cik",
    "filing_date",
    "filing_url",
    "film_number",
    "form_type",
    "investment_discretion",
    "issuer_name",
    "market_value",
    "other_managers",
    "period",
    "put_call",
    "shares_or_principal_amount",
    "shares_or_principal_type",
    "title_of_class",
    "voting_authority_none",
    "voting_authority_shared",
    "voting_authority_sole",
]


class Massive13FProvider(InstitutionalHoldingsProvider):
    """Fetch holding-level SEC Form 13F data from Massive."""

    BASE_URL = "https://api.massive.com"

    def __init__(self, api_key: str | None) -> None:
        self.api_key = api_key
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    def fetch_13f(
        self,
        filing_date_gte: str | None = None,
        filing_date_lte: str | None = None,
        filer_cik: str | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        params: dict[str, object] = {"limit": limit, "sort": "filing_date.desc"}
        if filing_date_gte:
            params["filing_date.gte"] = filing_date_gte
        if filing_date_lte:
            params["filing_date.lte"] = filing_date_lte
        if filer_cik:
            params["filer_cik"] = filer_cik

        frames: list[pd.DataFrame] = []
        url: str | None = f"{self.BASE_URL}{MASSIVE_13F_PATH}"

        while url:
            resp = requests.get(
                url,
                headers=self._headers,
                params=params if url.endswith(MASSIVE_13F_PATH) else None,
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()

            results = payload.get("results", [])
            if results:
                frames.append(pd.DataFrame(results))

            url = payload.get("next_url")
            params = {}

        if not frames:
            return pd.DataFrame(columns=RAW_13F_COLUMNS)

        df = pd.concat(frames, ignore_index=True)
        for col in RAW_13F_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        return df[RAW_13F_COLUMNS]
