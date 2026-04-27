from __future__ import annotations
from typing import Any
import logging
import sys

from app.governance.policy_registry import PolicyRegistry, TablePolicy

# Configure logging format to include timestamps
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class PolicyAgent:
    def __init__(self, registry: PolicyRegistry) -> None:
        self.registry = registry

    # ADDED trace_id parameter
    def validate_tables_for_sql(self, trace_id: str, tables: list[tuple[str,str]]) -> dict[str, Any]:
        logger.info(f"--- [PolicyAgent] [Trace: {trace_id}] Validating access for {len(tables)} tables ---")
        
        allowed, denied = [], []
        for ds, tb in tables:
            # PASSED trace_id to the registry
            p = self.registry.get(trace_id, ds, tb)
            if not p:
                logger.warning(f"--- [PolicyAgent] [Trace: {trace_id}] DENIED: No policy found for {ds}.{tb} ---")
                denied.append({"dataset": ds, "table": tb, "reason": "NO_POLICY_FOUND"})
            elif not p.allow_sql:
                logger.warning(f"--- [PolicyAgent] [Trace: {trace_id}] DENIED: allow_sql is FALSE for {ds}.{tb} ---")
                denied.append({"dataset": ds, "table": tb, "reason": "ALLOW_SQL_FALSE"})
            else:
                logger.info(f"--- [PolicyAgent] [Trace: {trace_id}] ALLOWED: SQL execution permitted for {ds}.{tb} ---")
                allowed.append({"dataset": ds, "table": tb, "policy": p.__dict__})
                
        return {"allowed": allowed, "denied": denied}

    def validate_join(self, left: TablePolicy, right: TablePolicy) -> tuple[bool, str]:
        if left.join_group == right.join_group:
            return True, "SAME_JOIN_GROUP"
        if left.allow_cross_group_join and right.allow_cross_group_join:
            return True, "CROSS_GROUP_ALLOWED"
        return False, "CROSS_GROUP_DENIED"

    def can_synthesize(self, policies: list[TablePolicy]) -> tuple[bool, str]:
        if any(not p.allow_synthesis for p in policies):
            return False, "SYNTHESIS_DENIED_BY_POLICY"
        return True, "SYNTHESIS_ALLOWED"