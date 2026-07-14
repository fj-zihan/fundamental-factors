"""Massive REST API implementation for 13F filings."""

from __future__ import annotations

import time

import pandas as pd
import requests

from ..config import (
    MASSIVE_13F_MAX_PAGES,
    MASSIVE_13F_PATH,
    MASSIVE_LOG_EVERY_N_PAGES,
    MASSIVE_PAGE_SLEEP_SECONDS,
    MASSIVE_REQUEST_TIMEOUT_SECONDS,
)
from .base import InstitutionalHoldingsProvider
from .request_utils import massive_rate_limited, retry_with_linear_backoff


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

    def __init__(
        self,
        api_key: str | None,
        max_pages: int | None = MASSIVE_13F_MAX_PAGES,
    ) -> None:
        self.api_key = api_key
        self.max_pages = max_pages
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
        page_count = 0

        while url:
            resp = self._get(
                url,
                params=params if url.endswith(MASSIVE_13F_PATH) else None,
            )
            payload = resp.json()
            page_count += 1

            results = payload.get("results", [])
            if results:
                frames.append(pd.DataFrame(results))

            url = payload.get("next_url")
            params = {}
            if (
                MASSIVE_LOG_EVERY_N_PAGES > 0
                and page_count % MASSIVE_LOG_EVERY_N_PAGES == 0
            ):
                rows_so_far = sum(len(frame) for frame in frames)
                print(
                    f"Massive 13F fetch progress: {page_count} pages, "
                    f"{rows_so_far} rows"
                )
            if self.max_pages and page_count >= self.max_pages:
                break
            if url and MASSIVE_PAGE_SLEEP_SECONDS > 0:
                time.sleep(MASSIVE_PAGE_SLEEP_SECONDS)

        if not frames:
            return pd.DataFrame(columns=RAW_13F_COLUMNS)

        df = pd.concat(frames, ignore_index=True)
        for col in RAW_13F_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        return df[RAW_13F_COLUMNS]

    def _get(
        self,
        url: str,
        params: dict[str, object] | None = None,
    ) -> requests.Response:
        return self._request(url, params=params)

    @retry_with_linear_backoff
    @massive_rate_limited
    def _request(
        self,
        url: str,
        params: dict[str, object] | None = None,
    ) -> requests.Response:
        return requests.get(
            url,
            headers=self._headers,
            params=params,
            timeout=MASSIVE_REQUEST_TIMEOUT_SECONDS,
        )
