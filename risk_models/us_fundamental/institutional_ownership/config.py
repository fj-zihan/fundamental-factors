"""Configuration for the institutional ownership factor pipeline."""

from __future__ import annotations

import os


# Massive 13F API
MASSIVE_13F_PATH = "/stocks/filings/vX/13-F"
MASSIVE_13F_LIMIT = 1000
FILING_LOOKBACK_DAYS = int(os.getenv("IO_FILING_LOOKBACK_DAYS", "120"))
PERIOD_LOOKBACK_DAYS = int(os.getenv("IO_PERIOD_LOOKBACK_DAYS", "1095"))
MASSIVE_13F_MAX_PAGES = (
    int(os.getenv("MASSIVE_13F_MAX_PAGES"))
    if os.getenv("MASSIVE_13F_MAX_PAGES")
    else None
)

# Massive market-cap denominator API. The MVP uses market cap to support a real
# historical panel. TODO: switch to point-in-time float market cap when a
# historical float source is available.
MASSIVE_TICKER_DETAILS_PATH_TEMPLATE = "/v3/reference/tickers/{ticker}"
MASSIVE_MAX_RETRIES = int(os.getenv("MASSIVE_MAX_RETRIES", "5"))
MASSIVE_RETRY_SLEEP_SECONDS = float(os.getenv("MASSIVE_RETRY_SLEEP_SECONDS", "10"))
MASSIVE_PAGE_SLEEP_SECONDS = float(os.getenv("MASSIVE_PAGE_SLEEP_SECONDS", "0.25"))
MASSIVE_REQUEST_TIMEOUT_SECONDS = float(
    os.getenv("MASSIVE_REQUEST_TIMEOUT_SECONDS", "30")
)
MASSIVE_REQUESTS_PER_MINUTE = float(os.getenv("MASSIVE_REQUESTS_PER_MINUTE", "5"))
MASSIVE_LOG_EVERY_N_PAGES = int(os.getenv("MASSIVE_LOG_EVERY_N_PAGES", "10"))

# Static denominator fallback. This file is intentionally not required at import
# time; the provider raises a clear error if a run needs it and it is missing.
DENOMINATOR_DATA_PATH = os.getenv(
    "DENOMINATOR_DATA_PATH",
    "data/static/denominator_data.csv",
)

# Factor construction
WINSOR_LOW = 0.01
WINSOR_HIGH = 0.99
FACTOR_MAX_TICKERS = (
    int(os.getenv("IO_FACTOR_MAX_TICKERS"))
    if os.getenv("IO_FACTOR_MAX_TICKERS")
    else None
)

# Data quality floors
MIN_DENOMINATOR_COVERAGE = 0.80
