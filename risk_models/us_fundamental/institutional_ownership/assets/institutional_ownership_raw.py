"""Raw 13F holdings asset."""

from datetime import datetime, timedelta

import dagster as dg
import pandas as pd

from ..config import FILING_LOOKBACK_DAYS, MASSIVE_13F_LIMIT, PERIOD_LOOKBACK_DAYS
from ..infra.base import InstitutionalHoldingsProvider


REQUIRED_RAW_COLUMNS = {
    "filer_cik",
    "filing_date",
    "period",
    "cusip",
    "issuer_name",
    "market_value",
    "shares_or_principal_amount",
    "shares_or_principal_type",
    "put_call",
    "form_type",
}


@dg.asset(
    group_name="institutional_ownership",
    description="Raw SEC Form 13F holding-level data from Massive.",
)
def institutional_ownership_raw(
    context: dg.AssetExecutionContext,
    sp500_universe: pd.DataFrame,
    io_provider: dg.ResourceParam[InstitutionalHoldingsProvider],
) -> pd.DataFrame:
    cutoff = (datetime.today() - timedelta(days=FILING_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    context.log.info(
        f"Fetching 13F filings since {cutoff}; universe has {len(sp500_universe)} tickers"
    )

    df = io_provider.fetch_13f(
        filing_date_gte=cutoff,
        limit=MASSIVE_13F_LIMIT,
    )

    missing = REQUIRED_RAW_COLUMNS - set(df.columns)
    if missing:
        raise dg.Failure(
            description=f"institutional_ownership_raw missing columns: {sorted(missing)}",
            metadata={"missing_columns": sorted(missing)},
        )

    period_cutoff = pd.Timestamp(datetime.today() - timedelta(days=PERIOD_LOOKBACK_DAYS))
    df = df[pd.to_datetime(df["period"], errors="coerce") >= period_cutoff].copy()

    context.log.info(
        f"Fetched {len(df)} 13F rows, "
        f"{df['filer_cik'].nunique() if not df.empty else 0} filers, "
        f"{df['period'].nunique() if not df.empty else 0} periods"
    )
    return df
