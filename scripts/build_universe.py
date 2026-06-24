import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "../data/static/SP500_universe.csv")


def scrape_sp500() -> pd.DataFrame:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    table = soup.find("table", {"id": "constituents"})
    rows = table.find_all("tr")
    data = [
        [td.get_text(strip=True) for td in row.find_all("td")]
        for row in rows if row.find_all("td")
    ]
    return pd.DataFrame(
        data,
        columns=["Symbol", "Security", "GICS Sector", "GICS Sub-Industry",
                 "HQ", "Date added", "CIK", "Founded"]
    )


if __name__ == "__main__":
    df = scrape_sp500()
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(df)} tickers to {OUTPUT_PATH}")
