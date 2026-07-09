"""Institutional ownership MVP factor asset."""

import numpy as np
import dagster as dg
import pandas as pd

from ..config import MIN_FLOAT_COVERAGE, WINSOR_HIGH, WINSOR_LOW
from ..infra.base import FloatDataProvider


OUTPUT_COLUMNS = [
    "ticker",
    "period",
    "asof_date",
    "io_level",
    "io_breadth",
    "holder_count",
    "total_13f_market_value",
    "float_market_cap",
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
    description="MVP institutional ownership factors: float-scaled level and breadth.",
)
def institutional_ownership_factor(
    context: dg.AssetExecutionContext,
    institutional_holdings_normalized: pd.DataFrame,
    float_provider: dg.ResourceParam[FloatDataProvider],
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

    float_data = float_provider.fetch_float_data(
        tickers=grouped["ticker"].dropna().astype(str).unique().tolist(),
        periods=grouped["period"].dropna().tolist(),
    )
    float_data = float_data.copy()
    float_data["period"] = pd.to_datetime(float_data["period"], errors="coerce")
    float_data["ticker"] = float_data["ticker"].astype(str).str.upper()

    result = grouped.merge(
        float_data[["ticker", "period", "float_market_cap"]],
        on=["ticker", "period"],
        how="left",
    )

    coverage = result["float_market_cap"].notna().mean()
    if coverage < MIN_FLOAT_COVERAGE:
        raise dg.Failure(
            description=(
                f"Float market cap coverage {coverage:.1%} is below the "
                f"MVP floor of {MIN_FLOAT_COVERAGE:.0%}."
            ),
            metadata={"float_coverage": coverage, "rows": len(result)},
        )

    result = result.dropna(subset=["float_market_cap"]).copy()
    result["float_market_cap"] = pd.to_numeric(result["float_market_cap"], errors="coerce")
    result = result[result["float_market_cap"] > 0].copy()
    if result.empty:
        raise dg.Failure(description="No rows have positive float_market_cap.")

    result["io_level_raw"] = result["total_13f_market_value"] / result["float_market_cap"]
    result["io_breadth_raw"] = np.log1p(result["holder_count"])
    result["io_level"] = _zscore_by_period(result, "io_level_raw", "io_level")
    result["io_breadth"] = _zscore_by_period(result, "io_breadth_raw", "io_breadth")

    context.log.info(
        f"Computed IO factor for {len(result)} ticker-period rows, "
        f"{result['ticker'].nunique()} tickers, {result['period'].nunique()} periods"
    )
    return result[OUTPUT_COLUMNS].reset_index(drop=True)
