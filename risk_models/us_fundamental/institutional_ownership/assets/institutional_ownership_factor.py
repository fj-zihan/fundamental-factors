"""Institutional ownership MVP factor asset."""

import numpy as np
import dagster as dg
import pandas as pd

from ..config import (
    FACTOR_MAX_TICKERS,
    MIN_DENOMINATOR_COVERAGE,
    WINSOR_HIGH,
    WINSOR_LOW,
)
from ..infra.base import DenominatorDataProvider


OUTPUT_COLUMNS = [
    "ticker",
    "period",
    "asof_date",
    "io_level",
    "io_breadth",
    "holder_count",
    "total_13f_market_value",
    "denominator_value",
    "denominator_type",
    "market_cap",
]


def _zscore_by_period(df: pd.DataFrame, raw_col: str, out_col: str) -> pd.Series:
    def _transform(x: pd.Series) -> pd.Series:
        low = x.quantile(WINSOR_LOW)
        high = x.quantile(WINSOR_HIGH)
        clipped = x.clip(lower=low, upper=high)
        sigma = clipped.std()
        if sigma == 0 or pd.isna(sigma):
            return pd.Series(0.0, index=x.index)
        return (clipped - clipped.mean()) / sigma

    return df.groupby("period")[raw_col].transform(_transform)


@dg.asset(
    group_name="institutional_ownership",
    io_manager_key="io_manager",
    description="MVP institutional ownership factors: market-cap-scaled level and breadth.",
)
def institutional_ownership_factor(
    context: dg.AssetExecutionContext,
    institutional_holdings_normalized: pd.DataFrame,
    denominator_provider: dg.ResourceParam[DenominatorDataProvider],
) -> pd.DataFrame:
    if institutional_holdings_normalized.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    holdings = institutional_holdings_normalized.copy()
    grouped = (
        holdings.groupby(["ticker", "period"], as_index=False)
        .agg(
            total_13f_market_value=("market_value", "sum"),
            holder_count=("filer_cik", "nunique"),
            asof_date=("filing_date", "max"),
        )
    )
    if FACTOR_MAX_TICKERS:
        keep_tickers = sorted(grouped["ticker"].dropna().unique())[:FACTOR_MAX_TICKERS]
        grouped = grouped[grouped["ticker"].isin(keep_tickers)].copy()
        context.log.info(
            f"Limiting IO factor smoke run to {len(keep_tickers)} tickers "
            f"because IO_FACTOR_MAX_TICKERS={FACTOR_MAX_TICKERS}"
        )

    denominator_data = denominator_provider.fetch_denominator_data(
        tickers=grouped["ticker"].dropna().astype(str).unique().tolist(),
        periods=grouped["period"].dropna().tolist(),
    )
    denominator_data = denominator_data.copy()
    denominator_data["period"] = pd.to_datetime(
        denominator_data["period"], errors="coerce"
    )
    denominator_data["ticker"] = denominator_data["ticker"].astype(str).str.upper()

    result = grouped.merge(
        denominator_data[
            ["ticker", "period", "denominator_value", "denominator_type", "market_cap"]
        ],
        on=["ticker", "period"],
        how="left",
    )

    coverage = result["denominator_value"].notna().mean()
    if coverage < MIN_DENOMINATOR_COVERAGE:
        raise dg.Failure(
            description=(
                f"Denominator coverage {coverage:.1%} is below the "
                f"MVP floor of {MIN_DENOMINATOR_COVERAGE:.0%}."
            ),
            metadata={"denominator_coverage": coverage, "rows": len(result)},
        )

    result = result.dropna(subset=["denominator_value"]).copy()
    result["denominator_value"] = pd.to_numeric(
        result["denominator_value"], errors="coerce"
    )
    result = result[result["denominator_value"] > 0].copy()
    if result.empty:
        raise dg.Failure(description="No rows have positive denominator_value.")

    # TODO: Replace market_cap with point-in-time float market cap when a
    # historical float source is available.
    result["io_level_raw"] = (
        result["total_13f_market_value"] / result["denominator_value"]
    )
    result["io_breadth_raw"] = np.log1p(result["holder_count"])
    result["io_level"] = _zscore_by_period(result, "io_level_raw", "io_level")
    result["io_breadth"] = _zscore_by_period(result, "io_breadth_raw", "io_breadth")

    context.log.info(
        f"Computed IO factor for {len(result)} ticker-period rows, "
        f"{result['ticker'].nunique()} tickers, {result['period'].nunique()} periods"
    )
    return result[OUTPUT_COLUMNS].reset_index(drop=True)
