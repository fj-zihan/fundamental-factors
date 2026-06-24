"""
S3 IO Manager — live and backfill lineage modes.

S3 path schema
--------------
LIVE      s3://{bucket}/model=us_fundamental/dataset={dataset}/lineage=live/latest.parquet
BACKFILL  s3://{bucket}/model=us_fundamental/dataset={dataset}/lineage=backfill/build={build_ts}/data.parquet

build_ts (backfill only)
------------------------
Derived from context.run_start_time — UTC timestamp shared by all assets in
the same Dagster run. Human-readable and time-sortable: e.g. 20241231T120000.
"""

from __future__ import annotations

import dagster as dg
import pandas as pd

from .. import config
class S3RiskModelIOManager(dg.ConfigurableIOManager):
    """
    Parameters
    ----------
    bucket  : S3 bucket name (env: S3_BUCKET)
    lineage : "live" | "backfill"
    """

    bucket:  str = config.S3_BUCKET
    lineage: str = "live"

    def _path(self, context: dg.OutputContext | dg.InputContext) -> str:
        dataset = context.asset_key.path[-1]
        base    = f"s3://{self.bucket}/model={config.S3_MODEL}/dataset={dataset}"

        if self.lineage == "live":
            return f"{base}/lineage=live/latest.parquet"

        # backfill: versioned by run_start_time (stable + sortable across all assets in run)
        build_ts = context.run_start_time.strftime("%Y%m%dT%H%M%S")
        return f"{base}/lineage=backfill/build={build_ts}/data.parquet"

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None or obj.empty:
            context.log.warning(f"Empty DataFrame for {context.asset_key} — skipping S3 write")
            return
        path = self._path(context)
        context.log.info(f"[{self.lineage}] Writing {len(obj)} rows → {path}")
        obj.to_parquet(path, index=False)

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        path = self._path(context)
        context.log.info(f"[{self.lineage}] Reading ← {path}")
        return pd.read_parquet(path)
import os
import pandas as pd
import dagster as dg


class LocalRiskModelIOManager(dg.ConfigurableIOManager):
    base_path: str = "./data"

    def _path(self, context):
        dataset = context.asset_key.path[-1]
        return os.path.join(self.base_path, f"{dataset}.parquet")

    def handle_output(self, context, obj: pd.DataFrame) -> None:
        if obj is None or obj.empty:
            context.log.warning("Empty DF — skipping write")
            return

        path = self._path(context)
        obj.to_parquet(path, index=False)
        context.log.info(f"[LOCAL] write → {path}")

    def load_input(self, context) -> pd.DataFrame:
        path = self._path(context)
        context.log.info(f"[LOCAL] read ← {path}")
        return pd.read_parquet(path)
