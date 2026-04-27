from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import time
import logging
import sys

from app.tools.bigquery_tools import BigQueryTools
from app.config import settings

# Configure logging format to include timestamps
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@dataclass
class TablePolicy:
    dataset_id: str
    table_id: str
    allow_sql: bool
    allow_agg_only: bool
    allow_synthesis: bool
    join_group: str
    allow_cross_group_join: bool
    pii_flag: bool
    classification: str

class PolicyRegistry:
    def __init__(self, bq: BigQueryTools) -> None:
        self.bq = bq
        self._cache: dict[str, TablePolicy] = {}
        self._loaded_at = 0.0

    def _key(self, dataset_id: str, table_id: str) -> str:
        return f"{dataset_id}.{table_id}"

    # ADDED trace_id parameter with default fallback
    def load(self, force: bool = False, trace_id: str = "SYSTEM_INIT") -> None:
        if not force and (time.time() - self._loaded_at) < 60:
            return

        logger.info(f"--- [PolicyRegistry] [Trace: {trace_id}] Loading Policy Registry from BigQuery ---")

        sql = f"""
        SELECT
          dataset_id, table_id,
          classification, pii_flag,
          allow_sql, allow_agg_only, allow_synthesis,
          join_group, allow_cross_group_join
        FROM `{settings.policy_table}`
        """
        
        # PASSED trace_id down to BigQueryTools
        res = self.bq.execute_sql(trace_id, sql, max_rows=50000).rows
        
        self._cache.clear()
        for r in res:
            p = TablePolicy(
                dataset_id=r["dataset_id"],
                table_id=r["table_id"],
                classification=r.get("classification","INTERNAL"),
                pii_flag=bool(r.get("pii_flag", False)),
                allow_sql=bool(r.get("allow_sql", True)),
                allow_agg_only=bool(r.get("allow_agg_only", False)),
                allow_synthesis=bool(r.get("allow_synthesis", True)),
                join_group=str(r.get("join_group","default")),
                allow_cross_group_join=bool(r.get("allow_cross_group_join", False)),
            )
            self._cache[self._key(p.dataset_id, p.table_id)] = p

        self._loaded_at = time.time()
        logger.info(f"--- [PolicyRegistry] [Trace: {trace_id}] Successfully loaded {len(self._cache)} policies into cache ---")

    # ADDED trace_id parameter
    def get(self, trace_id: str, dataset_id: str, table_id: str) -> TablePolicy | None:
        # Pass trace_id into load()
        self.load(trace_id=trace_id)
        return self._cache.get(self._key(dataset_id, table_id))