from __future__ import annotations
from typing import Any
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

class BQStatsAgent:
    def __init__(self, bq: BigQueryTools) -> None:
        self.bq = bq

    # ADDED trace_id parameter
    def safe_query(self, trace_id: str, sql: str) -> dict[str, Any]:
        logger.info(f"--- [BQStatsAgent] [Trace: {trace_id}] Estimating query cost (Dry Run) ---")
        
        # 1. Pass trace_id to dry_run_bytes
        bytes_est = self.bq.dry_run_bytes(trace_id, sql)
        
        if bytes_est > settings.max_bq_bytes:
            logger.warning(f"--- [BQStatsAgent] [Trace: {trace_id}] Query blocked: DRY_RUN_BYTES_EXCEEDED ({bytes_est} bytes) ---")
            return {"ok": False, "error": "DRY_RUN_BYTES_EXCEEDED", "dry_run_bytes": bytes_est}

        logger.info(f"--- [BQStatsAgent] [Trace: {trace_id}] Dry run passed. Est bytes: {bytes_est}. Executing... ---")
        
        # 2. Pass trace_id to execute_sql
        res = self.bq.execute_sql(trace_id, sql, max_rows=settings.max_rows)
        
        return {
            "ok": True,
            "dry_run_bytes": bytes_est,
            "job_id": res.job_id,
            "bytes_processed": res.bytes_processed,
            "elapsed_ms": res.elapsed_ms,
            "rows": res.rows
        }