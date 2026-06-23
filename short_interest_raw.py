"""
short_interest_raw
==================
Fetches raw FINRA short interest data from the configured provider
for the full S&P 500 universe and writes to S3.
Fetch only — no factor construction here.
"""

from datetime import datetime, timedelta

import dagster as dg
import pandas as pd

from ..config import API_LIMIT, LOOKBACK_DAYS
from ..infra.base import ShortInterestProvider


@dg.asset(
    group_name="short_interest",
    description=(
        "Raw FINRA short interest from Massive API. "
        "One row per ticker × settlement_date. Written to S3."
    ),
)
def short_interest_raw(
    context: dg.AssetExecutionContext,
    sp500_universe: pd.DataFrame,
    si_provider: dg.ResourceParam[ShortInterestProvider],
) -> pd.DataFrame:
    """
    Output schema
    -------------
    ticker, settlement_date, short_interest, avg_daily_volume, days_to_cover
    """
    tickers = sp500_universe["ticker"].tolist()
    cutoff  = (datetime.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    context.log.info(f"Fetching SI for {len(tickers)} tickers from {cutoff}")

    df = si_provider.fetch(
        tickers             = tickers,
        settlement_date_gte = cutoff,
        limit               = API_LIMIT,
    )

    context.log.info(
        f"Fetched {len(df)} records, "
        f"{df['ticker'].nunique()} tickers, "
        f"{df['settlement_date'].nunique()} settlement dates"
    )
    return df
