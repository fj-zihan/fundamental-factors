import os
import dagster as dg
import pandas as pd


STATIC_PATH = os.path.join(
    os.path.dirname(__file__),
    "../../../../data/static/SP500_universe.csv",
)


@dg.asset(
    group_name="universe",
    description="S&P 500 constituent universe from local static file.",
)
def sp500_universe(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Output schema: ticker, company, sector, added
    """
    df_raw = pd.read_csv(STATIC_PATH)

    df = pd.DataFrame({
        "ticker":  df_raw["Symbol"].str.replace(".", "-", regex=False),
        "company": df_raw["Security"],
        "sector":  df_raw["GICS Sector"],
        "added":   df_raw.get("Date added", pd.Series(dtype=str)),
    })

    context.log.info(f"S&P 500 universe loaded from file: {len(df)} tickers")
    return df
