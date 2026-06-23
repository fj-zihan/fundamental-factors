"""
Universe asset: S&P 500 constituents.

Downloads the current S&P 500 ticker list from Wikipedia.
Note: current snapshot only — not point-in-time (PiT upgrade planned).
"""

import dagster as dg
import pandas as pd


@dg.asset(
    group_name="universe",
    description="S&P 500 constituent universe from Wikipedia. Non-PiT snapshot.",
)
def sp500_universe(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Output schema: ticker, company, sector, added
    """
    url    = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    sp500  = tables[0]

    df = pd.DataFrame({
        "ticker":  sp500["Symbol"].str.replace(".", "-", regex=False),
        "company": sp500["Security"],
        "sector":  sp500["GICS Sector"],
        "added":   sp500.get("Date added", pd.Series(dtype=str)),
    })

    context.log.info(f"S&P 500 universe: {len(df)} tickers")
    return df
