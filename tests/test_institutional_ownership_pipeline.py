import pandas as pd
from dagster import build_asset_context

from risk_models.us_fundamental.institutional_ownership.assets.institutional_holdings_normalized import (
    institutional_holdings_normalized,
)
from risk_models.us_fundamental.institutional_ownership.assets.institutional_ownership_factor import (
    institutional_ownership_factor,
)
from risk_models.us_fundamental.institutional_ownership.infra.float_data import (
    MassiveFloatDataProvider,
)


class _FloatProvider:
    def fetch_float_data(self, tickers, periods):
        return pd.DataFrame(
            {
                "ticker": ["AMZN", "MSFT", "NVDA"],
                "period": pd.to_datetime(["2024-09-30", "2024-09-30", "2024-09-30"]),
                "float_market_cap": [1000.0, 500.0, 250.0],
            }
        )


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FakeSession:
    def get(self, url, headers=None, params=None, timeout=None):
        if url.endswith("/stocks/vX/float"):
            return _FakeResponse(
                {
                    "results": [
                        {
                            "ticker": params["ticker"],
                            "free_float": 100.0,
                            "effective_date": "2026-07-01",
                        }
                    ]
                }
            )

        return _FakeResponse(
            {
                "results": [
                    {
                        "c": 25.0,
                        "t": 1711929600000,
                    }
                ]
            }
        )


def _raw_13f():
    return pd.DataFrame(
        [
            {
                "filer_cik": "1",
                "filing_date": "2024-11-14",
                "period": "2024-09-30",
                "cusip": "023135106",
                "issuer_name": "AMAZON COM INC",
                "market_value": 100.0,
                "shares_or_principal_amount": 10,
                "shares_or_principal_type": "SH",
                "put_call": None,
                "form_type": "13F-HR",
                "accession_number": "a",
            },
            {
                "filer_cik": "2",
                "filing_date": "2024-11-14",
                "period": "2024-09-30",
                "cusip": "023135106",
                "issuer_name": "AMAZON COM INC",
                "market_value": 200.0,
                "shares_or_principal_amount": 20,
                "shares_or_principal_type": "SH",
                "put_call": None,
                "form_type": "13F-HR",
                "accession_number": "b",
            },
            {
                "filer_cik": "3",
                "filing_date": "2024-11-14",
                "period": "2024-09-30",
                "cusip": "594918104",
                "issuer_name": "MICROSOFT CORP",
                "market_value": 150.0,
                "shares_or_principal_amount": 15,
                "shares_or_principal_type": "SH",
                "put_call": None,
                "form_type": "13F-HR",
                "accession_number": "c",
            },
            {
                "filer_cik": "4",
                "filing_date": "2024-11-14",
                "period": "2024-09-30",
                "cusip": "67066G104",
                "issuer_name": "NVIDIA CORP",
                "market_value": 100.0,
                "shares_or_principal_amount": 5,
                "shares_or_principal_type": "SH",
                "put_call": "CALL",
                "form_type": "13F-HR",
                "accession_number": "d",
            },
        ]
    )


def _universe():
    return pd.DataFrame(
        {
            "ticker": ["AMZN", "MSFT", "NVDA"],
            "company": ["Amazon.com Inc.", "Microsoft Corp", "NVIDIA Corp"],
        }
    )


def test_normalized_filters_options_and_maps_tickers():
    result = institutional_holdings_normalized(
        build_asset_context(),
        _raw_13f(),
        _universe(),
    )

    assert set(result["ticker"]) == {"AMZN", "MSFT"}
    assert "NVDA" not in set(result["ticker"])
    assert set(result.columns) == {
        "filer_cik",
        "period",
        "filing_date",
        "ticker",
        "cusip",
        "issuer_name",
        "market_value",
        "shares",
        "source_accession_number",
    }


def test_factor_uses_float_market_cap_denominator():
    normalized = institutional_holdings_normalized(
        build_asset_context(),
        _raw_13f(),
        _universe(),
    )
    result = institutional_ownership_factor(
        build_asset_context(),
        normalized,
        _FloatProvider(),
    )

    amzn = result[result["ticker"] == "AMZN"].iloc[0]
    msft = result[result["ticker"] == "MSFT"].iloc[0]

    assert amzn["total_13f_market_value"] == 300.0
    assert amzn["float_market_cap"] == 1000.0
    assert msft["total_13f_market_value"] == 150.0
    assert msft["float_market_cap"] == 500.0
    assert {"io_level", "io_breadth"}.issubset(result.columns)


def test_massive_float_provider_builds_float_market_cap():
    provider = MassiveFloatDataProvider(api_key="test", session=_FakeSession())

    result = provider.fetch_float_data(
        tickers=["AMZN"],
        periods=[pd.Timestamp("2024-03-31")],
    )

    row = result.iloc[0]
    assert row["ticker"] == "AMZN"
    assert row["free_float"] == 100.0
    assert row["close_price"] == 25.0
    assert row["float_market_cap"] == 2500.0
