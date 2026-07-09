"""Denominator providers for institutional ownership factors."""

from __future__ import annotations

import os
import time

import pandas as pd
import requests

from ..config import (
    DENOMINATOR_DATA_PATH,
    MASSIVE_MAX_RETRIES,
    MASSIVE_REQUEST_SLEEP_SECONDS,
    MASSIVE_RETRY_SLEEP_SECONDS,
    MASSIVE_TICKER_DETAILS_PATH_TEMPLATE,
)
from .base import DenominatorDataProvider


OUTPUT_COLUMNS = [
    "ticker",
    "period",
    "denominator_value",
    "denominator_type",
    "market_cap",
    "asof_date",
]


class StaticDenominatorDataProvider(DenominatorDataProvider):
    """
    CSV-backed denominator provider for local tests and fallback runs.

    Required columns:
      ticker, period, market_cap

    Alternative column:
      denominator_value
    """

    def __init__(self, path: str = DENOMINATOR_DATA_PATH) -> None:
        self.path = path

    def fetch_denominator_data(
        self,
        tickers: list[str],
        periods: list[pd.Timestamp],
    ) -> pd.DataFrame:
        if not os.path.exists(self.path):
            raise FileNotFoundError(
                f"Denominator data file not found: {self.path}. "
                "Provide DENOMINATOR_DATA_PATH or create data/static/denominator_data.csv "
                "with ticker, period, and market_cap."
            )

        df = pd.read_csv(self.path)
        required = {"ticker", "period"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing denominator data columns: {sorted(missing)}")

        df = df.copy()
        df["period"] = pd.to_datetime(df["period"], errors="coerce").dt.normalize()
        df["ticker"] = df["ticker"].astype(str).str.upper()

        if "denominator_value" not in df.columns:
            if "market_cap" not in df.columns:
                raise ValueError(
                    "Denominator data must include market_cap or denominator_value."
                )
            df["denominator_value"] = df["market_cap"]

        if "market_cap" not in df.columns:
            df["market_cap"] = df["denominator_value"]
        if "denominator_type" not in df.columns:
            df["denominator_type"] = "market_cap"
        if "asof_date" not in df.columns:
            df["asof_date"] = df["period"]

        keep_periods = (
            pd.to_datetime(pd.Series(periods), errors="coerce")
            .dropna()
            .dt.normalize()
            .unique()
        )
        out = df[
            df["ticker"].isin([t.upper() for t in tickers])
            & df["period"].isin(keep_periods)
        ].copy()
        return _normalize_output(out)


class MassiveMarketCapDataProvider(DenominatorDataProvider):
    """
    Massive-backed market-cap denominator provider.

    The MVP intentionally uses market cap, rather than latest free float, to
    produce a real historical 13F panel. Market cap is an imperfect denominator
    because it includes non-float shares.

    TODO: Replace this provider with point-in-time float market cap when a
    historical float source becomes available.
    """

    BASE_URL = "https://api.massive.com"

    def __init__(
        self,
        api_key: str | None,
        fallback: DenominatorDataProvider | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key
        self.fallback = fallback
        self.session = session or requests.Session()
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    def fetch_denominator_data(
        self,
        tickers: list[str],
        periods: list[pd.Timestamp],
    ) -> pd.DataFrame:
        clean_tickers = sorted({str(t).upper() for t in tickers if pd.notna(t)})
        clean_periods = (
            pd.to_datetime(pd.Series(periods), errors="coerce")
            .dropna()
            .dt.normalize()
            .drop_duplicates()
            .sort_values()
            .tolist()
        )

        rows: list[dict[str, object]] = []
        for ticker in clean_tickers:
            for period in clean_periods:
                record = self._fetch_market_cap(ticker, pd.Timestamp(period))
                self._sleep_between_requests()
                if not record:
                    continue

                rows.append(
                    {
                        "ticker": ticker,
                        "period": pd.Timestamp(period).normalize(),
                        "denominator_value": record["market_cap"],
                        "denominator_type": "market_cap",
                        "market_cap": record["market_cap"],
                        "asof_date": record.get("asof_date") or period,
                    }
                )

        api_data = pd.DataFrame(rows)
        if self.fallback is None:
            return _normalize_output(api_data)

        fallback_data = self._fetch_fallback_for_missing(
            api_data=api_data,
            tickers=clean_tickers,
            periods=clean_periods,
        )
        combined = pd.concat([api_data, fallback_data], ignore_index=True)
        return _normalize_output(combined)

    def _fetch_market_cap(
        self,
        ticker: str,
        period: pd.Timestamp,
    ) -> dict[str, object] | None:
        path = MASSIVE_TICKER_DETAILS_PATH_TEMPLATE.format(ticker=ticker)
        payload = self._get_json(
            f"{self.BASE_URL}{path}",
            params={"date": period.strftime("%Y-%m-%d")},
        )
        record = self._first_record(payload)
        if record is None:
            return None

        market_cap = _coerce_number(record.get("market_cap") or record.get("marketCap"))
        if market_cap is None:
            return None

        return {
            "market_cap": market_cap,
            "asof_date": record.get("as_of_date")
            or record.get("asof_date")
            or record.get("date"),
        }

    def _get_json(
        self,
        url: str,
        params: dict[str, object] | None = None,
    ) -> dict[str, object]:
        last_response: requests.Response | None = None
        for attempt in range(MASSIVE_MAX_RETRIES + 1):
            response = self.session.get(
                url,
                headers=self._headers,
                params=params,
                timeout=30,
            )
            last_response = response
            if getattr(response, "status_code", 200) != 429:
                response.raise_for_status()
                return response.json()

            retry_after = response.headers.get("Retry-After")
            if retry_after:
                sleep_seconds = float(retry_after)
            else:
                sleep_seconds = MASSIVE_RETRY_SLEEP_SECONDS * (attempt + 1)
            time.sleep(sleep_seconds)

        if last_response is None:
            raise RuntimeError("Massive market-cap request failed before a response.")
        last_response.raise_for_status()
        return last_response.json()

    @staticmethod
    def _first_record(payload: dict[str, object]) -> dict[str, object] | None:
        results = payload.get("results")
        if isinstance(results, list) and results:
            return results[0]
        if isinstance(results, dict):
            return results
        if payload.get("market_cap") or payload.get("ticker"):
            return payload
        return None

    @staticmethod
    def _sleep_between_requests() -> None:
        if MASSIVE_REQUEST_SLEEP_SECONDS > 0:
            time.sleep(MASSIVE_REQUEST_SLEEP_SECONDS)

    def _fetch_fallback_for_missing(
        self,
        api_data: pd.DataFrame,
        tickers: list[str],
        periods: list[pd.Timestamp],
    ) -> pd.DataFrame:
        if self.fallback is None:
            return pd.DataFrame()

        if api_data.empty:
            try:
                return self.fallback.fetch_denominator_data(
                    tickers=tickers, periods=periods
                )
            except FileNotFoundError:
                return pd.DataFrame()

        api_keys = set(zip(api_data["ticker"], api_data["period"]))
        missing_pairs = [
            (ticker, pd.Timestamp(period).normalize())
            for ticker in tickers
            for period in periods
            if (ticker, pd.Timestamp(period).normalize()) not in api_keys
        ]
        if not missing_pairs:
            return pd.DataFrame()

        missing_tickers = sorted({ticker for ticker, _ in missing_pairs})
        missing_periods = sorted({period for _, period in missing_pairs})
        try:
            fallback_data = self.fallback.fetch_denominator_data(
                tickers=missing_tickers,
                periods=missing_periods,
            )
        except FileNotFoundError:
            return pd.DataFrame()

        if fallback_data.empty:
            return fallback_data

        fallback_data = fallback_data.copy()
        fallback_data["ticker"] = fallback_data["ticker"].astype(str).str.upper()
        fallback_data["period"] = pd.to_datetime(
            fallback_data["period"], errors="coerce"
        ).dt.normalize()
        missing_key_set = set(missing_pairs)
        mask = [
            (ticker, period) in missing_key_set
            for ticker, period in zip(fallback_data["ticker"], fallback_data["period"])
        ]
        return fallback_data[mask].copy()


def _coerce_number(value: object) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    return float(number)


def _normalize_output(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    out = df.copy()
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out["period"] = pd.to_datetime(out["period"], errors="coerce").dt.normalize()
    out["denominator_value"] = pd.to_numeric(
        out["denominator_value"], errors="coerce"
    )

    for col in OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    return out[OUTPUT_COLUMNS].drop_duplicates(["ticker", "period"], keep="first")
