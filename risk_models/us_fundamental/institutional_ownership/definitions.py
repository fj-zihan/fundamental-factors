"""Dagster definitions for institutional ownership MVP factors."""

from __future__ import annotations

import os

import dagster as dg
from dotenv import load_dotenv

load_dotenv()

from risk_models.us_fundamental.shared.data.universe import sp500_universe
from risk_models.us_fundamental.shared.infra.io_manager import S3RiskModelIOManager

from .assets.institutional_holdings_normalized import institutional_holdings_normalized
from .assets.institutional_ownership_factor import institutional_ownership_factor
from .assets.institutional_ownership_raw import institutional_ownership_raw
from .config import FLOAT_DATA_PATH
from .infra.float_data import MassiveFloatDataProvider, StaticFloatDataProvider
from .infra.massive_13f import Massive13FProvider


resources = {
    "io_manager": S3RiskModelIOManager(lineage="live"),
    "io_provider": Massive13FProvider(api_key=os.getenv("MASSIVE_API_KEY")),
    "float_provider": MassiveFloatDataProvider(
        api_key=os.getenv("MASSIVE_API_KEY"),
        fallback=StaticFloatDataProvider(path=FLOAT_DATA_PATH),
    ),
}


institutional_ownership_mvp_job = dg.define_asset_job(
    name="institutional_ownership_mvp_job",
    selection=[
        "sp500_universe",
        "institutional_ownership_raw",
        "institutional_holdings_normalized",
        "institutional_ownership_factor",
    ],
)


defs = dg.Definitions(
    assets=[
        sp500_universe,
        institutional_ownership_raw,
        institutional_holdings_normalized,
        institutional_ownership_factor,
    ],
    jobs=[institutional_ownership_mvp_job],
    resources=resources,
)
