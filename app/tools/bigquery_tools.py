from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Iterable, Dict
import time
import sys
from google.cloud import bigquery

from app.config import settings
# Import your new JSON logger
from app.core.logger import get_gcp_logger

# Initialize the GCP-compatible structured logger
logger = get_gcp_logger("BigQueryTools")

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
    execute_sql / forecast interfaces (which exist per ADK docs).
    """
    def __init__(self) -> None:
        self.client = bigquery.Client(project=settings.project_id)

    def dry_run_bytes(self, trace_id: str, sql: str) -> int:
        logger.info("Executing dry_run_bytes", extra={"trace_id": trace_id})
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self.client.query(sql, job_config=job_config)
        return int(job.total_bytes_processed or 0)

    def validate_sql(self, trace_id: str, sql: str) -> Dict[str, Any]:
        """
        Performs a DRY RUN to validate SQL syntax and schema usage.
        Cost: Free (no bytes processed).
        """
        logger.info("Validating SQL Syntax & Safety", extra={"trace_id": trace_id})
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        
        try:
            job = self.client.query(sql, job_config=job_config)
            
            # --- GUARDRAIL 1: Statement Type Check ---
            # Ensure strictly SELECT. Block UPDATE, DELETE, DROP, INSERT, MERGE, etc.
            if job.statement_type != "SELECT":
                logger.warning(
                    f"Security Violation: Non-SELECT statement detected ({job.statement_type})", 
                    extra={"trace_id": trace_id}
                )
                return {
                    "valid": False, 
                    "error": f"Security Violation: Only SELECT statements are allowed. Detected: {job.statement_type}"
                }

            # If we get here, syntax and columns are valid
            logger.info(f"SQL Validation Passed. Statement: {job.statement_type}", extra={"trace_id": trace_id})
            return {"valid": True, "error": None}
            
        except Exception as e:
            # Return the raw error from BQ (e.g. "Column 'x' not found")
            logger.error(f"SQL Validation Failed: {e}", extra={"trace_id": trace_id})
            return {"valid": False, "error": str(e)}

    def safe_query(self, trace_id: str, sql: str) -> Dict[str, Any]:
        """
        Executes the query ONLY if it passes validation.
        """
        logger.info("Executing safe_query", extra={"trace_id": trace_id})
        
        # 1. Validate first (Double check safety) - Passing trace_id down
        validation = self.validate_sql(trace_id, sql)
        if not validation["valid"]:
            logger.warning("safe_query aborted due to failed validation", extra={"trace_id": trace_id})
            return {"ok": False, "error": validation["error"]}

        # 2. Execute
        try:
            query_job = self.client.query(sql) # standard config
            result = query_job.result() # waits for job to complete
            
            # Serialize rows
            rows = [dict(row) for row in result]
            
            logger.info(
                f"safe_query successful. Rows returned: {result.total_rows}, Bytes: {query_job.total_bytes_processed}", 
                extra={"trace_id": trace_id}
            )
            
            return {
                "ok": True,
                "job_id": query_job.job_id,
                "total_rows": result.total_rows,
                "bytes_processed": query_job.total_bytes_processed,
                "result": rows
            }
        except Exception as e:
            logger.error(f"Execution failed: {e}", extra={"trace_id": trace_id})
            return {"ok": False, "error": str(e)}
            

    def execute_sql(self, trace_id: str, sql: str, max_rows: int | None = None) -> BQResult:
        logger.info("Executing execute_sql directly", extra={"trace_id": trace_id})
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
        
        logger.info(
            f"execute_sql complete in {elapsed}ms. Bytes: {job.total_bytes_processed}", 
            extra={"trace_id": trace_id}
        )
        
        return BQResult(
            rows=rows,
            job_id=job.job_id,
            bytes_processed=int(job.total_bytes_processed or 0),
            elapsed_ms=elapsed,
        )

    def list_tables(self, trace_id: str, dataset_id: str) -> list[str]:
        logger.info(f"Listing tables for dataset: {dataset_id}", extra={"trace_id": trace_id})
        ds_ref = bigquery.DatasetReference(settings.project_id, dataset_id)
        return [t.table_id for t in self.client.list_tables(ds_ref)]