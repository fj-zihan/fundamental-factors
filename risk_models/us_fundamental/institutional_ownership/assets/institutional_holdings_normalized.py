"""Normalize raw 13F holdings for stock-level factor construction."""

import re

import dagster as dg
import pandas as pd


OUTPUT_COLUMNS = [
    "filer_cik",
    "period",
    "filing_date",
    "ticker",
    "cusip",
    "issuer_name",
    "market_value",
    "shares",
    "source_accession_number",
]


def _clean_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value).upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    for token in [" INC", " CORP", " CORPORATION", " CO", " LTD", " PLC", " COM", " CLASS"]:
        text = text.replace(token, " ")
    return re.sub(r"\s+", " ", text).strip()


def _attach_ticker(df: pd.DataFrame, sp500_universe: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ticker" in out.columns and out["ticker"].notna().any():
        out["ticker"] = out["ticker"].astype(str).str.upper()
        return out

    universe = sp500_universe[["ticker", "company"]].copy()
    universe["ticker"] = universe["ticker"].astype(str).str.upper()
    universe["issuer_key"] = universe["company"].map(_clean_name)

    out["issuer_key"] = out["issuer_name"].map(_clean_name)
    out = out.merge(universe[["ticker", "issuer_key"]], on="issuer_key", how="left")
    return out.drop(columns=["issuer_key"])


@dg.asset(
    group_name="institutional_ownership",
    description="Cleaned common-equity-like 13F holdings mapped to the universe.",
)
def institutional_holdings_normalized(
    context: dg.AssetExecutionContext,
    institutional_ownership_raw: pd.DataFrame,
    sp500_universe: pd.DataFrame,
) -> pd.DataFrame:
    if institutional_ownership_raw.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = institutional_ownership_raw.copy()
    df = df[df["put_call"].isna()].copy()
    df = df[df["shares_or_principal_type"].astype(str).str.upper() == "SH"].copy()
    df = _attach_ticker(df, sp500_universe)
    df = df[df["ticker"].notna()].copy()

    if df.empty:
        raise dg.Failure(
            description=(
                "No 13F rows could be mapped to the universe. "
                "Provide ticker in raw data or add a stronger CUSIP/security mapping."
            ),
            metadata={"raw_rows": len(institutional_ownership_raw)},
        )

    df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce")
    df["shares"] = pd.to_numeric(df["shares_or_principal_amount"], errors="coerce")
    df["period"] = pd.to_datetime(df["period"], errors="coerce")
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df["source_accession_number"] = df.get("accession_number", pd.NA)

    df = df.dropna(subset=["filer_cik", "period", "ticker", "market_value"])
    df = (
        df.sort_values(["filing_date", "source_accession_number"])
        .drop_duplicates(subset=["filer_cik", "period", "ticker"], keep="last")
    )

    result = df[OUTPUT_COLUMNS].reset_index(drop=True)
    context.log.info(
        f"Normalized {len(result)} 13F holdings across "
        f"{result['ticker'].nunique()} tickers and {result['period'].nunique()} periods"
    )
    return result
