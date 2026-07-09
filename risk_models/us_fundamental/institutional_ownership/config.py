"""Configuration for the institutional ownership factor pipeline."""

from __future__ import annotations

import os


# Massive 13F API
MASSIVE_13F_PATH = "/stocks/filings/vX/13-F"
MASSIVE_13F_LIMIT = 1000
FILING_LOOKBACK_DAYS = 120
MASSIVE_13F_MAX_PAGES = (
    int(os.getenv("MASSIVE_13F_MAX_PAGES"))
    if os.getenv("MASSIVE_13F_MAX_PAGES")
    else None
)

# Massive float and price APIs. The float endpoint provides latest free float
# shares, while the aggregate endpoint provides close prices used to construct
# float market cap.
MASSIVE_FLOAT_PATH = "/stocks/vX/float"
MASSIVE_AGGS_PATH_TEMPLATE = "/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
PRICE_LOOKBACK_DAYS = 7
MASSIVE_MAX_RETRIES = int(os.getenv("MASSIVE_MAX_RETRIES", "5"))
MASSIVE_RETRY_SLEEP_SECONDS = float(os.getenv("MASSIVE_RETRY_SLEEP_SECONDS", "2"))
MASSIVE_PAGE_SLEEP_SECONDS = float(os.getenv("MASSIVE_PAGE_SLEEP_SECONDS", "0.25"))

# Static float data fallback. This file is intentionally not required at import
# time; the provider raises a clear error if a run needs it and it is missing.
FLOAT_DATA_PATH = os.getenv("FLOAT_DATA_PATH", "data/static/float_data.csv")

# Factor construction
WINSOR_LOW = 0.01
WINSOR_HIGH = 0.99

# Data quality floors
MIN_FLOAT_COVERAGE = 0.80
