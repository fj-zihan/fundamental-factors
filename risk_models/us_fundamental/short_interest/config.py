"""
Central configuration — no magic numbers anywhere else.
"""

# S3
S3_BUCKET      = "risk-models"
S3_MODEL       = "us_fundamental"
S3_FREQUENCY   = "biweekly"

# Universe
UNIVERSE_NAME  = "sp500"

# API
LOOKBACK_DAYS  = 90
API_LIMIT      = 50_000

# Factor construction
WINSOR_LOW     = 0.01
WINSOR_HIGH    = 0.99
