from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Iterable, Dict # <--- Added Dict to prevent NameError
import time
import logging # <--- Added logging
import sys # <--- Added sys for stdout
from google.cloud import bigquery

from app.config import settings

# Configure logging format to include timestamps
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@dataclass
class BQResult:
    rows: list[dict[str, Any]]
    job_id: str | None
    bytes_processed: int | None
    elapsed_ms: int

class BigQueryTools:
    """
    Uses google-cloud-bigquery directly.
    If you install ADK BigQuery toolset later, you can swap this class to ADK’s
    execute_sql / forecast interfaces (which exist per ADK docs). :contentReference[oaicite:4]{index=4}
    """
    def __init__(self) -> None:
        self.client = bigquery.Client(project=settings.project_id)

    # ADDED trace_id parameter
    def dry_run_bytes(self, trace_id: str, sql: str) -> int:
        logger.info(f"--- [BigQueryTools] [Trace: {trace_id}] Executing dry_run_bytes ---")
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self.client.query(sql, job_config=job_config)
        return int(job.total_bytes_processed or 0)

    # ADDED trace_id parameter
    def validate_sql(self, trace_id: str, sql: str) -> Dict[str, Any]:
        """
        Performs a DRY RUN to validate SQL syntax and schema usage.
        Cost: Free (no bytes processed).
        """
        logger.info(f"--- [BigQueryTools] [Trace: {trace_id}] Validating SQL Syntax & Safety ---")
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        
        try:
            job = self.client.query(sql, job_config=job_config)
            
            # --- GUARDRAIL 1: Statement Type Check ---
            # Ensure strictly SELECT. Block UPDATE, DELETE, DROP, INSERT, MERGE, etc.
            if job.statement_type != "SELECT":
                logger.warning(f"--- [BigQueryTools] [Trace: {trace_id}] Security Violation: Non-SELECT statement detected ({job.statement_type}) ---")
                return {
                    "valid": False, 
                    "error": f"Security Violation: Only SELECT statements are allowed. Detected: {job.statement_type}"
                }

            # If we get here, syntax and columns are valid
            logger.info(f"--- [BigQueryTools] [Trace: {trace_id}] SQL Validation Passed. Statement: {job.statement_type} ---")
            return {"valid": True, "error": None}
            
        except Exception as e:
            # Return the raw error from BQ (e.g. "Column 'x' not found")
            logger.error(f"--- [BigQueryTools] [Trace: {trace_id}] SQL Validation Failed: {e} ---")
            return {"valid": False, "error": str(e)}

    # ADDED trace_id parameter
    def safe_query(self, trace_id: str, sql: str) -> Dict[str, Any]:
        """
        Executes the query ONLY if it passes validation.
        """
        logger.info(f"--- [BigQueryTools] [Trace: {trace_id}] Executing safe_query ---")
        
        # 1. Validate first (Double check safety) - Passing trace_id down
        validation = self.validate_sql(trace_id, sql)
        if not validation["valid"]:
            logger.warning(f"--- [BigQueryTools] [Trace: {trace_id}] safe_query aborted due to failed validation ---")
            return {"ok": False, "error": validation["error"]}

        # 2. Execute
        try:
            query_job = self.client.query(sql) # standard config
            result = query_job.result() # waits for job to complete
            
            # Serialize rows
            rows = [dict(row) for row in result]
            
            logger.info(f"--- [BigQueryTools] [Trace: {trace_id}] safe_query successful. Rows returned: {result.total_rows}, Bytes: {query_job.total_bytes_processed} ---")
            
            return {
                "ok": True,
                "job_id": query_job.job_id,
                "total_rows": result.total_rows,
                "bytes_processed": query_job.total_bytes_processed,
                "result": rows
            }
        except Exception as e:
            logger.error(f"--- [BigQueryTools] [Trace: {trace_id}] Execution failed: {e} ---")
            return {"ok": False, "error": str(e)}
            

    # ADDED trace_id parameter
    def execute_sql(self, trace_id: str, sql: str, max_rows: int | None = None) -> BQResult:
        logger.info(f"--- [BigQueryTools] [Trace: {trace_id}] Executing execute_sql directly ---")
        t0 = time.time()
        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        job = self.client.query(sql, job_config=job_config)
        rows_iter = job.result(page_size=min(max_rows or settings.max_rows, 10000))
        rows: list[dict[str, Any]] = []
        for i, r in enumerate(rows_iter):
            if max_rows and i >= max_rows:
                break
            rows.append(dict(r.items()))
        elapsed = int((time.time() - t0) * 1000)
        
        logger.info(f"--- [BigQueryTools] [Trace: {trace_id}] execute_sql complete in {elapsed}ms. Bytes: {job.total_bytes_processed} ---")
        
        return BQResult(
            rows=rows,
            job_id=job.job_id,
            bytes_processed=int(job.total_bytes_processed or 0),
            elapsed_ms=elapsed,
        )

    # ADDED trace_id parameter
    def list_tables(self, trace_id: str, dataset_id: str) -> list[str]:
        logger.info(f"--- [BigQueryTools] [Trace: {trace_id}] Listing tables for dataset: {dataset_id} ---")
        ds_ref = bigquery.DatasetReference(settings.project_id, dataset_id)
        return [t.table_id for t in self.client.list_tables(ds_ref)]
