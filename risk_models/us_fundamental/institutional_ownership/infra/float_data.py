"""Float denominator providers for institutional ownership factors."""

from __future__ import annotations

import os
import time
from datetime import timedelta

import pandas as pd
import requests

from ..config import (
    FLOAT_DATA_PATH,
    MASSIVE_AGGS_PATH_TEMPLATE,
    MASSIVE_FLOAT_PATH,
    MASSIVE_MAX_RETRIES,
    PRICE_LOOKBACK_DAYS,
    MASSIVE_RETRY_SLEEP_SECONDS,
)
from .base import FloatDataProvider


class StaticFloatDataProvider(FloatDataProvider):
    """
    CSV-backed float data provider for MVP wiring and local tests.

    Required columns:
      ticker, period, float_market_cap

    Alternative columns:
      ticker, period, float_shares, price
    """

    def __init__(self, path: str = FLOAT_DATA_PATH) -> None:
        self.path = path

    def fetch_float_data(
        self,
        tickers: list[str],
        periods: list[pd.Timestamp],
    ) -> pd.DataFrame:
        if not os.path.exists(self.path):
            raise FileNotFoundError(
                f"Float data file not found: {self.path}. "
                "Provide FLOAT_DATA_PATH or create data/static/float_data.csv "
                "with ticker, period, and float_market_cap."
            )

        df = pd.read_csv(self.path)
        required = {"ticker", "period"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing float data columns: {sorted(missing)}")

        df = df.copy()
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        df["ticker"] = df["ticker"].astype(str).str.upper()

        if "float_market_cap" not in df.columns:
            if {"float_shares", "price"}.issubset(df.columns):
                df["float_market_cap"] = df["float_shares"] * df["price"]
            else:
                raise ValueError(
                    "Float data must include float_market_cap or both "
                    "float_shares and price."
                )

        keep_periods = pd.to_datetime(pd.Series(periods), errors="coerce").dropna().unique()
        out = df[
            df["ticker"].isin([t.upper() for t in tickers])
            & df["period"].isin(keep_periods)
        ].copy()

        cols = ["ticker", "period", "float_market_cap"]
        if "float_shares" in out.columns:
            cols.append("float_shares")
        return out[cols]


class MassiveFloatDataProvider(FloatDataProvider):
    """
    Massive-backed provider for float denominators.

    Massive's float endpoint provides latest free float shares rather than a
    historical float series. For the MVP, this provider pairs latest free float
    with period-end close prices to construct float_market_cap.
    """

    BASE_URL = "https://api.massive.com"

    def __init__(
        self,
        api_key: str | None,
        fallback: FloatDataProvider | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key
        self.fallback = fallback
        self.session = session or requests.Session()
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    def fetch_float_data(
        self,
        tickers: list[str],
        periods: list[pd.Timestamp],
    ) -> pd.DataFrame:
        clean_tickers = sorted({str(t).upper() for t in tickers if pd.notna(t)})
        clean_periods = (
            pd.to_datetime(pd.Series(periods), errors="coerce")
            .dropna()
            .drop_duplicates()
            .sort_values()
            .tolist()
        )

        rows: list[dict[str, object]] = []
        for ticker in clean_tickers:
            float_record = self._fetch_latest_free_float(ticker)
            if not float_record:
                continue

            free_float = float_record.get("free_float")
            if free_float is None or pd.isna(free_float):
                continue

            for period in clean_periods:
                price_record = self._fetch_close_price(ticker, pd.Timestamp(period))
                if not price_record:
                    continue

                close_price = price_record["close_price"]
                rows.append(
                    {
                        "ticker": ticker,
                        "period": pd.Timestamp(period).normalize(),
                        "free_float": free_float,
                        "effective_date": float_record.get("effective_date"),
                        "close_price": close_price,
                        "price_date": price_record.get("price_date"),
                        "float_market_cap": free_float * close_price,
                    }
                )

        api_data = pd.DataFrame(rows)
        if self.fallback is None:
            return self._normalize_output(api_data)

        fallback_data = self._fetch_fallback_for_missing(
            api_data=api_data,
            tickers=clean_tickers,
            periods=clean_periods,
        )
        combined = pd.concat([api_data, fallback_data], ignore_index=True)
        return self._normalize_output(combined)

    def _fetch_latest_free_float(self, ticker: str) -> dict[str, object] | None:
        payload = self._get_json(
            f"{self.BASE_URL}{MASSIVE_FLOAT_PATH}",
            params={"ticker": ticker},
        )
        record = self._first_record(payload)
        if record is None:
            return None

        free_float = self._coerce_number(
            record.get("free_float")
            or record.get("float")
            or record.get("freeFloat")
        )
        if free_float is None:
            return None

        return {
            "ticker": str(record.get("ticker", ticker)).upper(),
            "free_float": free_float,
            "effective_date": record.get("effective_date") or record.get("date"),
        }

    def _fetch_close_price(
        self,
        ticker: str,
        period: pd.Timestamp,
    ) -> dict[str, object] | None:
        to_date = period.normalize()
        from_date = to_date - timedelta(days=PRICE_LOOKBACK_DAYS)
        path = MASSIVE_AGGS_PATH_TEMPLATE.format(
            ticker=ticker,
            from_date=from_date.strftime("%Y-%m-%d"),
            to_date=to_date.strftime("%Y-%m-%d"),
        )
        payload = self._get_json(
            f"{self.BASE_URL}{path}",
            params={"adjusted": "true", "sort": "desc", "limit": 1},
        )
        record = self._first_record(payload)
        if record is None:
            return None

        close_price = self._coerce_number(record.get("c") or record.get("close"))
        if close_price is None:
            return None

        price_date = record.get("t") or record.get("timestamp")
        if price_date is not None:
            price_date = pd.to_datetime(price_date, unit="ms", errors="coerce")

        return {"close_price": close_price, "price_date": price_date}

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
            raise RuntimeError("Massive float request failed before a response was created.")
        last_response.raise_for_status()
        return last_response.json()

    @staticmethod
    def _first_record(payload: dict[str, object]) -> dict[str, object] | None:
        results = payload.get("results")
        if isinstance(results, list) and results:
            return results[0]
        if isinstance(results, dict):
            return results
        if payload.get("ticker") or payload.get("free_float"):
            return payload
        return None

    @staticmethod
    def _coerce_number(value: object) -> float | None:
        number = pd.to_numeric(value, errors="coerce")
        if pd.isna(number):
            return None
        return float(number)

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
                return self.fallback.fetch_float_data(tickers=tickers, periods=periods)
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
            fallback_data = self.fallback.fetch_float_data(
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

    @staticmethod
    def _normalize_output(df: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "ticker",
            "period",
            "float_market_cap",
            "free_float",
            "effective_date",
            "close_price",
            "price_date",
        ]
        if df.empty:
            return pd.DataFrame(columns=columns)

        out = df.copy()
        out["ticker"] = out["ticker"].astype(str).str.upper()
        out["period"] = pd.to_datetime(out["period"], errors="coerce").dt.normalize()
        out["float_market_cap"] = pd.to_numeric(
            out["float_market_cap"], errors="coerce"
        )

        for col in columns:
            if col not in out.columns:
                out[col] = pd.NA

        return out[columns].drop_duplicates(["ticker", "period"], keep="first")
