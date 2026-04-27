# import json
# import time
# import re
# import hashlib
# import traceback
# from dataclasses import dataclass
# from datetime import datetime, timezone
# from typing import Dict, List, Any, Optional, Tuple
# import os
# from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# from google.cloud import bigquery
# from google.cloud import storage
# from google.api_core.client_options import ClientOptions
# from google.cloud import discoveryengine_v1beta as discoveryengine

# import vertexai
# from vertexai.generative_models import GenerativeModel, GenerationConfig

# from dotenv import load_dotenv
# load_dotenv()


# # =========================================================
# # ENV CONFIG 
# # =========================================================
# BQ_TABLES_JSON = os.environ.get("BQ_TABLES", "").strip()  # JSON list of "project.dataset.table"
# BQ_SOURCES_JSON = os.environ.get("BQ_SOURCES", "").strip()  # JSON list of {"project","dataset"}

# TABLE_NAME_PREFIX = os.environ.get("TABLE_NAME_PREFIX", "").strip()
# MAX_TABLES = int(os.environ.get("MAX_TABLES", "0"))  # 0 = no limit

# GCS_BUCKET = os.environ.get("GCS_BUCKET", "qiddiya-metadata-rag") 
# GCS_PREFIX = os.environ.get("GCS_PREFIX", "bq_schema_cards")

# ENABLE_LLM = os.environ.get("ENABLE_LLM", "true").lower() == "true"
# VERTEX_PROJECT = os.environ.get("VERTEX_PROJECT", "")
# VERTEX_REGION = os.environ.get("VERTEX_REGION", "europe-west1")
# GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-pro")
# LLM_SLEEP_SEC = float(os.environ.get("LLM_SLEEP_SEC", "0.2"))

# DRY_RUN_VALIDATE = os.environ.get("DRY_RUN_VALIDATE", "true").lower() == "true"
# MAX_FIX_ATTEMPTS = int(os.environ.get("MAX_FIX_ATTEMPTS", "2"))

# REFRESH_SEARCH = os.environ.get("REFRESH_SEARCH", "true").lower() == "true"
# SEARCH_LOCATION = os.environ.get("SEARCH_LOCATION", "global")
# DATA_STORE_ID = os.environ.get("DATA_STORE_ID", "").strip()
# RECONCILIATION_MODE = os.environ.get("RECONCILIATION_MODE", "FULL")
# DATA_SCHEMA = os.environ.get("DATA_SCHEMA", "document")


# # Optional: deterministic profiling (kept OFF by default)
# ENABLE_PROFILING = os.environ.get("ENABLE_PROFILING", "false").lower() == "true"


# # =========================================================
# # Helpers
# # =========================================================
# def now_utc_iso() -> str:
#     return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

# def run_ts() -> str:
#     return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# def parse_json_list(env_val: str) -> List[Any]:
#     if not env_val:
#         return []
#     return json.loads(env_val)

# def parse_table_id(full: str) -> Tuple[str, str, str]:
#     parts = full.split(".")
#     if len(parts) != 3:
#         raise ValueError(f"Invalid table id '{full}'. Expected 'project.dataset.table'")
#     return parts[0], parts[1], parts[2]

# def sanitize_id(s: str) -> str:
#     raw = s
#     safe = re.sub(r"[^a-zA-Z0-9-_]", "_", raw)
#     h = hashlib.md5(raw.encode("utf-8")).hexdigest()[:8]
#     safe = (safe[:110] if len(safe) > 110 else safe)
#     return f"{safe}-{h}"

# def _sanitize_location(loc: str) -> str:
#     if not loc:
#         return "global"
#     loc = loc.strip().lower()
#     loc = loc.replace("“", "").replace("”", "").replace('"', "").replace("'", "")
#     loc = re.sub(r"\s+", "", loc)
#     return loc

# def build_keyword_boost(le: Dict[str, Any]) -> str:
#     words = []
#     for k in ("measures", "kpis", "dimensions"):
#         words.extend(le.get(k, []))
#     # de-dupe
#     seen, out = set(), []
#     for w in words:
#         w2 = (w or "").strip().lower()
#         if w2 and w2 not in seen:
#             seen.add(w2)
#             out.append((w or "").strip())
#     return " | ".join(out)


# # =========================================================
# # Toolbox Models
# # =========================================================
# @dataclass(frozen=True)
# class TableTarget:
#     project: str
#     dataset: str
#     table: str

#     @property
#     def fqtn(self) -> str:
#         return f"`{self.project}.{self.dataset}.{self.table}`"

#     @property
#     def raw_id(self) -> str:
#         return f"{self.project}.{self.dataset}.{self.table}"


# # =========================================================
# # BigQuery Toolbox (heavy usage)
# # =========================================================
# class BigQueryToolbox:
#     def __init__(self, client: bigquery.Client):
#         self.bq = client

#     def list_tables(self, project: str, dataset: str, prefix: str = "") -> List[str]:
#         tables = [t.table_id for t in self.bq.list_tables(f"{project}.{dataset}")]
#         if prefix:
#             tables = [t for t in tables if t.startswith(prefix)]
#         return sorted(tables)

#     def get_table(self, tgt: TableTarget) -> bigquery.Table:
#         ref = bigquery.TableReference(bigquery.DatasetReference(tgt.project, tgt.dataset), tgt.table)
#         return self.bq.get_table(ref)

#     def dry_run(self, sql: str) -> Tuple[bool, str, Optional[int]]:
#         try:
#             job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
#             job = self.bq.query(sql, job_config=job_config)
#             return True, "", int(job.total_bytes_processed) if job.total_bytes_processed is not None else None
#         except Exception as e:
#             return False, str(e), None

#     def get_join_relationships(self, tgt: TableTarget) -> List[Dict[str, Any]]:
#         """
#         Deterministically fetches enforced Foreign Key relationships from BigQuery INFORMATION_SCHEMA
#         and retrieves the parent table's high-level schema context.
#         """
#         sql = f"""
#         SELECT
#             kcu.column_name AS child_column,
#             ccu.table_catalog AS parent_project,
#             ccu.table_schema AS parent_dataset,
#             ccu.table_name AS parent_table,
#             ccu.column_name AS parent_column
#         FROM
#             `{tgt.project}.{tgt.dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
#         JOIN
#             `{tgt.project}.{tgt.dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
#             ON tc.constraint_name = kcu.constraint_name
#         JOIN
#             `{tgt.project}.{tgt.dataset}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` ccu
#             ON tc.constraint_name = ccu.constraint_name
#         WHERE
#             tc.constraint_type = 'FOREIGN KEY'
#             AND tc.table_name = '{tgt.table}'
#         """
        
#         ok, err, _ = self.dry_run(sql)
#         if not ok:
#             return [] # Skip if INFORMATION_SCHEMA isn't accessible or supported here
            
#         try:
#             query_job = self.bq.query(sql)
#             results = query_job.result()
            
#             joins = []
#             for row in results:
#                 join_info = {
#                     "child_column": row.child_column,
#                     "parent_table": f"{row.parent_project}.{row.parent_dataset}.{row.parent_table}",
#                     "parent_column": row.parent_column,
#                     "parent_schema": []
#                 }
                
#                 # Fetch parent schema details for LLM context
#                 try:
#                     parent_ref = bigquery.TableReference(
#                         bigquery.DatasetReference(row.parent_project, row.parent_dataset), 
#                         row.parent_table
#                     )
#                     parent_bq_table = self.bq.get_table(parent_ref)
                    
#                     join_info["parent_schema"] = [
#                         {
#                             "name": f.name, 
#                             "type": f.field_type, 
#                             "description": f.description or ""
#                         }
#                         for f in parent_bq_table.schema
#                     ]
#                 except Exception:
#                     pass # Silently proceed if parent table can't be fetched
                
#                 joins.append(join_info)
                
#             return joins
#         except Exception as e:
#             print(f"Failed to fetch joins for {tgt.raw_id}: {e}")
#             return []

#     def _get_column_profiles(self, tgt: TableTarget, schema) -> dict:
#         """
#         Builds a single optimized analytical query to extract:
#         - Strings/Bools: 5 unique sample values
#         - Numeric: Min, Max, Avg
#         - Dates: Min, Max
#         """
#         select_clauses = []
#         valid_fields = []
        
#         for idx, f in enumerate(schema):
#             # Skip complex types to avoid query crashes
#             if f.field_type in ('RECORD', 'STRUCT', 'ARRAY') or f.mode == 'REPEATED':
#                 continue
                
#             fname = f"`{f.name}`"
#             alias_base = f"c{idx}"  # Safe alias mapping
            
#             if f.field_type in ('STRING', 'BOOL'):
#                 select_clauses.append(f"ARRAY_AGG(DISTINCT CAST({fname} AS STRING) IGNORE NULLS LIMIT 5) AS `{alias_base}_samples`")
#                 valid_fields.append((f.name, 'categorical', alias_base))
#             elif f.field_type in ('INT64', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC'):
#                 select_clauses.append(f"CAST(ROUND(MIN({fname}), 0) AS STRING) AS `{alias_base}_min`")
#                 select_clauses.append(f"CAST(ROUND(MAX({fname}), 0) AS STRING) AS `{alias_base}_max`")
#                 select_clauses.append(f"CAST(ROUND(AVG({fname}), 0) AS STRING) AS `{alias_base}_avg`")
#                 valid_fields.append((f.name, 'numeric', alias_base))
#                 valid_fields.append((f.name, 'numeric', alias_base))
#             elif f.field_type in ('DATE', 'DATETIME', 'TIMESTAMP', 'TIME'):
#                 select_clauses.append(f"CAST(MIN({fname}) AS STRING) AS `{alias_base}_min`")
#                 select_clauses.append(f"CAST(MAX({fname}) AS STRING) AS `{alias_base}_max`")
#                 valid_fields.append((f.name, 'datetime', alias_base))
        
#         if not select_clauses:
#             return {}
            
#         sql = f"SELECT \n" + ",\n".join(select_clauses) + f"\nFROM {tgt.fqtn}"
        
#         ok, err, _ = self.dry_run(sql)
#         if not ok:
#             print(f"Skipping profiling for {tgt.raw_id} due to dry-run failure: {err}")
#             return {}
            
#         try:
#             rows = list(self.bq.query(sql).result())
#             if not rows: return {}
#             row = rows[0]
            
#             profile_data = {}
#             for fname, ftype, alias in valid_fields:
#                 p = {}
#                 if ftype == 'categorical':
#                     samples = getattr(row, f"{alias}_samples", [])
#                     p["sample_values"] = [str(s) for s in samples] if samples else []
#                 elif ftype == 'numeric':
#                     p["min"] = getattr(row, f"{alias}_min", None)
#                     p["max"] = getattr(row, f"{alias}_max", None)
#                     p["avg"] = getattr(row, f"{alias}_avg", None)
#                 elif ftype == 'datetime':
#                     p["min"] = getattr(row, f"{alias}_min", None)
#                     p["max"] = getattr(row, f"{alias}_max", None)
#                 profile_data[fname] = p
#             return profile_data
            
#         except Exception as e:
#             print(f"Profiling query failed for {tgt.raw_id}: {e}")
#             return {}

#     def build_table_card(self, tgt: TableTarget) -> Dict[str, Any]:
#         t = self.get_table(tgt)
        
#         # 1. FETCH DETAILED COLUMN STATS / SAMPLES
#         profiles = self._get_column_profiles(tgt, t.schema)

#         cols = []
#         policy_tag_count = 0
#         for f in t.schema:
#             col_info = {
#                 "name": f.name,
#                 "type": f.field_type,
#                 "mode": f.mode,
#                 "description": f.description or ""
#             }
            
#             # Merge the dynamically fetched min/max/avg/samples into the schema definition
#             if f.name in profiles:
#                 col_info.update(profiles[f.name])
                
#             cols.append(col_info)
            
#             if getattr(f, "policy_tags", None) and getattr(f.policy_tags, "names", None):
#                 policy_tag_count += len(f.policy_tags.names)

#         stats = {
#             "total_rows": int(t.num_rows) if t.num_rows is not None else None,
#             "bytes": int(t.num_bytes) if t.num_bytes is not None else None,
#             "created": datetime.fromtimestamp(t.created.timestamp(), tz=timezone.utc).replace(microsecond=0).isoformat()
#                         if getattr(t, "created", None) else None,
#             "last_modified": datetime.fromtimestamp(t.modified.timestamp(), tz=timezone.utc).replace(microsecond=0).isoformat()
#                         if getattr(t, "modified", None) else None,
#             "partitioning": {
#                 "type": getattr(t.time_partitioning, "type_", None),
#                 "field": getattr(t.time_partitioning, "field", None),
#             } if t.time_partitioning else None,
#             "clustering_fields": list(t.clustering_fields) if t.clustering_fields else None,
#         }

#         return {
#             "doc_id": tgt.raw_id,
#             "project": tgt.project,
#             "dataset": tgt.dataset,
#             "table": tgt.table,
#             "location": getattr(t, "location", None),
#             "description": t.description or "",
#             "columns": cols,
#             "stats": stats,
#             "governance": {"policy_tag_count": policy_tag_count},
#             "join_relationships": self.get_join_relationships(tgt),
#             "generated_at_utc": now_utc_iso(),
#         }

#     def infer_measures_dimensions(self, card: Dict[str, Any]) -> Dict[str, List[str]]:
#         colnames = [c["name"].lower() for c in card.get("columns", [])]
    
#         def has_any(substrs: List[str]) -> bool:
#             return any(any(s in c for s in substrs) for c in colnames)
    
#         measures, dims = [], []
    
#         # --- CONSTRUCTION & ASSETS ---
#         if has_any(["progress", "completion", "pct"]):
#             measures += ["percent complete", "completion rate"]
#         if has_any(["budget", "cost", "variance", "capex", "opex"]):
#             measures += ["total cost", "budget variance", "actual spend", "capex"]
        
#         # --- HR & HIRING ---
#         if has_any(["hc", "headcount", "staff", "employee"]):
#             measures += ["total headcount", "active employees"]
#             dims += ["department", "grade", "nationality", "gender"]
#         if has_any(["hired", "applicant", "vacancy", "recruitment"]):
#             measures += ["hiring velocity", "time to fill", "open positions"]
    
#         # --- GUEST & DIGITAL ---
#         if has_any(["nps", "csat", "score", "rating"]):
#             measures += ["customer satisfaction score", "net promoter score", "average rating"]
#         if has_any(["wait", "queue", "dwell"]):
#             measures += ["average wait time", "dwell time"]
    
#         # --- PROCUREMENT ---
#         if has_any(["vendor", "supplier", "contractor"]):
#             dims += ["vendor name", "supplier category"]
#         if has_any(["po_", "invoice", "spend"]):
#             measures += ["total spend", "procurement value"]
    
#         # --- IT & TECH ---
#         if has_any(["ticket", "incident", "severity"]):
#             measures += ["ticket volume", "resolution rate"]
#             dims += ["priority", "issue type", "system"]
    
#         # De-dupe and clean
#         measures = sorted(list(set([m.strip() for m in measures])))
#         dims = sorted(list(set([d.strip() for d in dims])))
    
#         return {"measures_inferred": measures, "dimensions_inferred": dims}
        
#     def optional_profile(self, tgt: TableTarget, card: Dict[str, Any]) -> Dict[str, Any]:
#         if not ENABLE_PROFILING:
#             return {}

#         cols = card.get("columns", [])
#         date_cols = [c["name"] for c in cols if c["type"] in ("DATE", "DATETIME", "TIMESTAMP")]
#         if not date_cols:
#             return {}

#         col = date_cols[0]
#         sql = f"SELECT MIN({col}) AS min_v, MAX({col}) AS max_v FROM {tgt.fqtn}"
#         ok, err, _ = self.dry_run(sql)
#         return {"date_range_col": col, "date_range_dryrun_ok": ok, "date_range_error": "" if ok else err}


# # =========================================================
# # LLM Toolbox
# # =========================================================
# class LLMToolbox:
#     def __init__(self, model_name: str):
#         self.model = GenerativeModel(model_name)

#     def prompt_for_table(self, card: Dict[str, Any]) -> str:
#         fqtn = f"`{card['project']}.{card['dataset']}.{card['table']}`"
        
#         # Inject our new sample values and stats into the LLM prompt so it writes better SQL
#         col_lines_list = []
#         for c in card["columns"]:
#             line = f"- {c['name']} ({c['type']}): {c.get('description','')}"
#             if c.get("sample_values"):
#                 line += f" [Samples: {', '.join(c['sample_values'])}]"
#             col_lines_list.append(line)
#         col_lines = "\n".join(col_lines_list)
        
#         joins_data = card.get("join_relationships", [])
#         joins_context = json.dumps(joins_data, indent=2) if joins_data else "No explicit database foreign keys found."

#         return f"""
# You are a senior analytics engineer. Return STRICT JSON ONLY (no markdown, no commentary).

# Table: {fqtn}
# Use ONLY the columns listed below.

# STRICT JSON KEYS (and ONLY these keys):
# - measures: 5-30 business measure phrases + synonyms (e.g., "items sold", "units sold", "quantity sold")
# - kpis: 10-15 KPI ideas (business phrasing)
# - dimensions: 10-20 grouping dimensions (business phrasing)
# - join_hints: 5-10 join/lineage hints (text only). Incorporate known explicit joins if provided below.
# - sample_qa: at least 10 entries, each has:
#   - question
#   - sql (valid BigQuery Standard SQL using {fqtn})
#   - answer_summary (ONE sentence)

# Rules:
# - Keep each SQL under 12 lines.
# - Use BigQuery Standard SQL only.
# - If explicit database joins are provided, utilize them to form accurate JOIN logic in sample_qa when applicable.
# - Do not include any extra keys.

# COLUMNS:
# {col_lines}

# KNOWN DATABASE JOINS:
# {joins_context}
# """.strip()

#     def prompt_fix_sql(self, card: Dict[str, Any], bad_sql: str, error: str) -> str:
#         fqtn = f"`{card['project']}.{card['dataset']}.{card['table']}`"
#         col_names = [c["name"] for c in card["columns"]]
#         return f"""
# Fix this BigQuery SQL to pass dry-run.

# Table: {fqtn}
# Valid columns: {col_names}

# Bad SQL:
# {bad_sql}

# Dry-run error:
# {error}

# Return STRICT JSON ONLY:
# {{"sql":"<fixed_full_sql>"}}
# """.strip()

#     @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
#     def call_json(self, prompt: str) -> Dict[str, Any]:
#         resp = self.model.generate_content(
#             prompt,
#             generation_config=GenerationConfig(
#                 temperature=0.2,
#                 max_output_tokens=8192,
#                 response_mime_type="application/json",
#             ),
#         )
#         raw = (resp.text or "").strip()
#         if not raw:
#             raise ValueError("Gemini returned empty response")
#         return json.loads(raw)

#     @staticmethod
#     def enforce_min_qa(enrichment: Dict[str, Any], min_n: int = 10) -> Dict[str, Any]:
#         enrichment.setdefault("measures", [])
#         enrichment.setdefault("kpis", [])
#         enrichment.setdefault("dimensions", [])
#         enrichment.setdefault("join_hints", [])
#         enrichment.setdefault("sample_qa", [])
#         if len(enrichment["sample_qa"]) < min_n:
#             raise ValueError(f"LLM returned {len(enrichment['sample_qa'])} QAs; need >= {min_n}")
#         return enrichment


# # =========================================================
# # Discovery Engine Toolbox
# # =========================================================
# class DiscoveryEngineToolbox:
#     def __init__(self, location: str, data_store_id: str):
#         self.loc = _sanitize_location(location)
#         self.data_store_id = data_store_id

#         if self.loc not in {"global", "us", "eu"}:
#             raise ValueError(f"Invalid SEARCH_LOCATION='{location}'. Use one of: global, us, eu")

#         if self.loc == "global":
#             self.client = discoveryengine.DocumentServiceClient()
#         else:
#             self.client = discoveryengine.DocumentServiceClient(
#                 client_options=ClientOptions(api_endpoint=f"{self.loc}-discoveryengine.googleapis.com")
#             )

#     def import_documents(self, project_for_search: str, gcs_uri: str):
#         if not self.data_store_id:
#             raise ValueError("DATA_STORE_ID is required when REFRESH_SEARCH=true")

#         parent = self.client.branch_path(
#             project=project_for_search,
#             location=self.loc,
#             data_store=self.data_store_id,
#             branch="default_branch",
#         )

#         mode = discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
#         if RECONCILIATION_MODE.upper() == "FULL":
#             mode = discoveryengine.ImportDocumentsRequest.ReconciliationMode.FULL

#         request = discoveryengine.ImportDocumentsRequest(
#             parent=parent,
#             gcs_source=discoveryengine.GcsSource(
#                 input_uris=[gcs_uri],
#                 data_schema=DATA_SCHEMA,  # for Document JSONL this must be 'document'
#             ),
#             reconciliation_mode=mode,
#         )

#         op = self.client.import_documents(request=request)
#         print(f"Refreshing Vertex Search datastore… op={op.operation.name}")
#         op.result()
#         print("✅ Search refresh completed.")


# # =========================================================
# # Search Text Builder 
# # =========================================================
# def build_search_text(card: Dict[str, Any]) -> str:
#     parts = []
#     parts.append(f"Project: {card.get('project')}")
#     parts.append(f"Dataset: {card.get('dataset')}")
#     parts.append(f"Table: {card.get('table')}")
#     parts.append(f"Description: {card.get('description','')}")
#     parts.append("Columns:")
    
#     # Render rich column stats so Vector Search can find semantic matches on exact data values
#     for c in card.get("columns", []):
#         base_line = f"- {c.get('name')} ({c.get('type')}): {c.get('description','')}"
        
#         extras = []
#         if c.get("sample_values"):
#             extras.append(f"Samples: {', '.join(c['sample_values'])}")
#         if c.get("min") is not None:
#             if c.get("avg") is not None:
#                 extras.append(f"Range: [{c['min']} to {c['max']}], Avg: {c['avg']}")
#             else:
#                 extras.append(f"Range: [{c['min']} to {c['max']}]")
        
#         if extras:
#             parts.append(f"{base_line} | {' | '.join(extras)}")
#         else:
#             parts.append(base_line)

#     # Injecting Join Relationships into searchable text
#     joins = card.get("join_relationships", [])
#     if joins:
#         parts.append("Explicit Database Joins (Foreign Keys):")
#         for j in joins:
#             parts.append(f"- Column {j.get('child_column')} joins to parent table {j.get('parent_table')} on column {j.get('parent_column')}.")

#     inf = card.get("toolbox_inference", {})
#     if inf:
#         parts.append("Measures (inferred):")
#         for m in inf.get("measures_inferred", []):
#             parts.append(f"- {m}")
#         parts.append("Dimensions (inferred):")
#         for d in inf.get("dimensions_inferred", []):
#             parts.append(f"- {d}")

#     le = card.get("llm_enrichment", {})
#     if le:
#         parts.append("Measures (canonical + synonyms):")
#         for m in le.get("measures", []):
#             parts.append(f"- {m}")
#         parts.append("Dimensions / group-by fields:")
#         for d in le.get("dimensions", []):
#             parts.append(f"- {d}")
#         parts.append("KPIs / metrics ideas:")
#         for k in le.get("kpis", []):
#             parts.append(f"- {k}")
#         parts.append("Join hints:")
#         for j in le.get("join_hints", []):
#             parts.append(f"- {j}")
#         parts.append("Sample Q&A:")
#         for qa in le.get("sample_qa", []):
#             parts.append(f"- Q: {qa.get('question')}")
#             parts.append(f"  SQL: {qa.get('sql')}")
#             parts.append(f"  Answer: {qa.get('answer_summary')}")

#     prof = card.get("profiling", {})
#     if prof:
#         parts.append("Profiling:")
#         for k, v in prof.items():
#             parts.append(f"- {k}: {v}")

#     return "\n".join(parts)


# # =========================================================
# # GCS Uploader Toolbox
# # =========================================================
# class GCSToolbox:
#     def __init__(self):
#         self.client = storage.Client()

#     def upload_jsonl(self, bucket_name: str, object_name: str, json_lines: List[str]) -> str:
#         bucket = self.client.bucket(bucket_name)
#         blob = bucket.blob(object_name)
#         blob.upload_from_string("\n".join(json_lines) + "\n", content_type="application/json")
#         return f"gs://{bucket_name}/{object_name}"


# # =========================================================
# # Target resolver
# # =========================================================
# def resolve_targets(bqt: BigQueryToolbox) -> List[TableTarget]:
#     targets: List[TableTarget] = []

#     if BQ_TABLES_JSON:
#         table_ids = parse_json_list(BQ_TABLES_JSON)
#         for tid in table_ids:
#             p, d, t = parse_table_id(tid)
#             targets.append(TableTarget(p, d, t))
#         return targets

#     if not BQ_SOURCES_JSON:
#         raise ValueError("Set either BQ_TABLES or BQ_SOURCES env var (JSON).")

#     sources = parse_json_list(BQ_SOURCES_JSON)
#     for s in sources:
#         p = s["project"]
#         d = s["dataset"]
#         tables = bqt.list_tables(p, d, prefix=TABLE_NAME_PREFIX)
#         for t in tables:
#             targets.append(TableTarget(p, d, t))

#     if MAX_TABLES > 0:
#         targets = targets[:MAX_TABLES]

#     return targets


# # =========================================================
# # MAIN
# # =========================================================
# def main():
#     print(f"[{now_utc_iso()}] Starting…")

#     bq = bigquery.Client()
#     bqt = BigQueryToolbox(bq)
#     gcs = GCSToolbox()

#     targets = resolve_targets(bqt)
#     print(f"Total tables to process: {len(targets)}")

#     llm: Optional[LLMToolbox] = None
#     if ENABLE_LLM:
#         if not VERTEX_PROJECT:
#             raise ValueError("VERTEX_PROJECT is required when ENABLE_LLM=true")
#         vertexai.init(project=VERTEX_PROJECT, location=VERTEX_REGION)
#         llm = LLMToolbox(GEMINI_MODEL)

#     de: Optional[DiscoveryEngineToolbox] = None
#     if REFRESH_SEARCH:
#         de = DiscoveryEngineToolbox(SEARCH_LOCATION, DATA_STORE_ID)

#     jsonl_lines: List[str] = []

#     for i, tgt in enumerate(targets, start=1):
#         print(f"({i}/{len(targets)}) {tgt.raw_id}")

#         card = bqt.build_table_card(tgt)

#         # Toolbox inference always
#         card["toolbox_inference"] = bqt.infer_measures_dimensions(card)

#         # Optional profiling
#         card["profiling"] = bqt.optional_profile(tgt, card)

#         # LLM enrichment
#         if llm:
#             try:
#                 enrichment = llm.call_json(llm.prompt_for_table(card))
#                 enrichment = llm.enforce_min_qa(enrichment, 10)

#                 # Validate/fix SQL via dry-run
#                 for qa in enrichment["sample_qa"]:
#                     sql = (qa.get("sql") or "").strip()
#                     if not sql:
#                         qa["dry_run_ok"] = False
#                         qa["dry_run_error"] = "Missing SQL"
#                         continue

#                     if not DRY_RUN_VALIDATE:
#                         qa["dry_run_ok"] = True
#                         qa["dry_run_bytes"] = None
#                         continue

#                     ok, err, bytes_processed = bqt.dry_run(sql)
#                     attempts = 0
#                     while (not ok) and attempts < MAX_FIX_ATTEMPTS:
#                         fix = llm.call_json(llm.prompt_fix_sql(card, sql, err))
#                         sql = (fix.get("sql") or sql).strip()
#                         ok, err, bytes_processed = bqt.dry_run(sql)
#                         attempts += 1

#                     qa["sql"] = sql
#                     qa["dry_run_ok"] = ok
#                     qa["dry_run_error"] = "" if ok else err
#                     qa["dry_run_bytes"] = bytes_processed
#                     time.sleep(LLM_SLEEP_SEC)

#                 card["llm_enrichment"] = enrichment
#                 card["llm_enrichment_error"] = ""
#             except RetryError as re_err:
#                 inner = re_err.last_attempt.exception()
#                 card["llm_enrichment"] = {}
#                 card["llm_enrichment_error"] = f"{type(inner).__name__}: {inner}"
#                 card["llm_enrichment_traceback"] = "".join(
#                     traceback.format_exception(type(inner), inner, inner.__traceback__)
#                 )[:4000]
#             except Exception as e:
#                 card["llm_enrichment"] = {}
#                 card["llm_enrichment_error"] = f"{type(e).__name__}: {e}"
#                 card["llm_enrichment_traceback"] = "".join(
#                     traceback.format_exception(type(e), e, e.__traceback__)
#                 )[:4000]

#         card["search_text"] = build_search_text(card)

#         # Structured datastore JSONL record (jsonData string)
#         safe_id = sanitize_id(card["doc_id"])

#         structured_record = {
#             "project": card["project"],
#             "dataset": card["dataset"],
#             "table": card["table"],
#             "location": card.get("location") or "",
#             "description": card.get("description") or "",
#             "columns": card.get("columns") or [],
#             "stats": card.get("stats") or {},
#             "generated_at_utc": card.get("generated_at_utc") or "",
#             "search_text": card.get("search_text") or "",

#             # Schema-safe additions
#             "join_relationships_json": json.dumps(card.get("join_relationships") or [], ensure_ascii=False),
#             "llm_enrichment_ok": bool(card.get("llm_enrichment")),
#             "llm_enrichment_error": card.get("llm_enrichment_error") or "",
#             "llm_enrichment_json": json.dumps(card.get("llm_enrichment") or {}, ensure_ascii=False),

#             "toolbox_inference_json": json.dumps(card.get("toolbox_inference") or {}, ensure_ascii=False),
#             "profiling_json": json.dumps(card.get("profiling") or {}, ensure_ascii=False),

#             # Used for retrieval boosting
#             "keyword_boost": build_keyword_boost(card.get("llm_enrichment") or {}),
#         }

#         doc = {
#             "id": safe_id,
#             "jsonData": json.dumps(structured_record, ensure_ascii=False)
#         }

#         jsonl_lines.append(json.dumps(doc, ensure_ascii=False))

#     object_name = f"{GCS_PREFIX}/bq_schema_cards_{run_ts()}.jsonl"
#     gcs_uri = gcs.upload_jsonl(GCS_BUCKET, object_name, jsonl_lines)
#     print(f"✅ Uploaded JSONL: {gcs_uri}")

#     if de:
#         # Use Vertex project if set, else fall back to first table's project
#         search_project = VERTEX_PROJECT or targets[0].project
#         de.import_documents(search_project, gcs_uri)

#     print(f"[{now_utc_iso()}] Done.")


# if __name__ == "__main__":
#     main()
########################################################New version ##########################################
import json
import time
import re
import hashlib
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import os
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

from google.cloud import bigquery
from google.cloud import storage
from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1beta as discoveryengine

import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

from dotenv import load_dotenv
load_dotenv()


# =========================================================
# ENV CONFIG 
# =========================================================
BQ_TABLES_JSON = os.environ.get("BQ_TABLES", "").strip()
BQ_SOURCES_JSON = os.environ.get("BQ_SOURCES", "").strip()

TABLE_NAME_PREFIX = os.environ.get("TABLE_NAME_PREFIX", "").strip()
MAX_TABLES = int(os.environ.get("MAX_TABLES", "0"))

GCS_BUCKET = os.environ.get("GCS_BUCKET", "qiddiya-metadata-rag")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "bq_schema_cards")

ENABLE_LLM = os.environ.get("ENABLE_LLM", "true").lower() == "true"
VERTEX_PROJECT = os.environ.get("VERTEX_PROJECT", "")
VERTEX_REGION = os.environ.get("VERTEX_REGION", "europe-west1")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-pro")
LLM_SLEEP_SEC = float(os.environ.get("LLM_SLEEP_SEC", "0.2"))

DRY_RUN_VALIDATE = os.environ.get("DRY_RUN_VALIDATE", "true").lower() == "true"
MAX_FIX_ATTEMPTS = int(os.environ.get("MAX_FIX_ATTEMPTS", "2"))

REFRESH_SEARCH = os.environ.get("REFRESH_SEARCH", "true").lower() == "true"
SEARCH_LOCATION = os.environ.get("SEARCH_LOCATION", "global")
DATA_STORE_ID = os.environ.get("DATA_STORE_ID", "").strip()
RECONCILIATION_MODE = os.environ.get("RECONCILIATION_MODE", "INCREMENTAL")
DATA_SCHEMA = os.environ.get("DATA_SCHEMA", "document")

ENABLE_PROFILING = os.environ.get("ENABLE_PROFILING", "false").lower() == "true"


# =========================================================
# Helpers
# =========================================================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def run_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def parse_json_list(env_val: str) -> List[Any]:
    if not env_val:
        return []
    return json.loads(env_val)

def parse_table_id(full: str) -> Tuple[str, str, str]:
    parts = full.split(".")
    if len(parts) != 3:
        raise ValueError(f"Invalid table id '{full}'. Expected 'project.dataset.table'")
    return parts[0], parts[1], parts[2]

def sanitize_id(s: str) -> str:
    raw = s
    safe = re.sub(r"[^a-zA-Z0-9-_]", "_", raw)
    h = hashlib.md5(raw.encode("utf-8")).hexdigest()[:8]
    safe = (safe[:110] if len(safe) > 110 else safe)
    return f"{safe}-{h}"

def _sanitize_location(loc: str) -> str:
    if not loc:
        return "global"
    loc = loc.strip().lower()
    loc = loc.replace("\u201c", "").replace("\u201d", "").replace('"', "").replace("'", "")
    loc = re.sub(r"\s+", "", loc)
    return loc

def build_keyword_boost(le: Dict[str, Any]) -> str:
    words = []
    for k in ("measures", "kpis", "dimensions"):
        words.extend(le.get(k, []))
    seen, out = set(), []
    for w in words:
        w2 = (w or "").strip().lower()
        if w2 and w2 not in seen:
            seen.add(w2)
            out.append((w or "").strip())
    return " | ".join(out)


# =========================================================
# Toolbox Models
# =========================================================
@dataclass(frozen=True)
class TableTarget:
    project: str
    dataset: str
    table: str

    @property
    def fqtn(self) -> str:
        return f"`{self.project}.{self.dataset}.{self.table}`"

    @property
    def raw_id(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"


# =========================================================
# BigQuery Toolbox
# =========================================================
class BigQueryToolbox:
    def __init__(self, client: bigquery.Client):
        self.bq = client

    def list_tables(self, project: str, dataset: str, prefix: str = "") -> List[str]:
        tables = [t.table_id for t in self.bq.list_tables(f"{project}.{dataset}")]
        if prefix:
            tables = [t for t in tables if t.startswith(prefix)]
        return sorted(tables)

    def get_table(self, tgt: TableTarget) -> bigquery.Table:
        ref = bigquery.TableReference(bigquery.DatasetReference(tgt.project, tgt.dataset), tgt.table)
        return self.bq.get_table(ref)

    def dry_run(self, sql: str) -> Tuple[bool, str, Optional[int]]:
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            job = self.bq.query(sql, job_config=job_config)
            return True, "", int(job.total_bytes_processed) if job.total_bytes_processed is not None else None
        except Exception as e:
            return False, str(e), None

    def get_join_relationships(self, tgt: TableTarget) -> List[Dict[str, Any]]:
        sql = f"""
        SELECT
            kcu.column_name AS child_column,
            ccu.table_catalog AS parent_project,
            ccu.table_schema AS parent_dataset,
            ccu.table_name AS parent_table,
            ccu.column_name AS parent_column
        FROM
            `{tgt.project}.{tgt.dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
        JOIN
            `{tgt.project}.{tgt.dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN
            `{tgt.project}.{tgt.dataset}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` ccu
            ON tc.constraint_name = ccu.constraint_name
        WHERE
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = '{tgt.table}'
        """

        ok, err, _ = self.dry_run(sql)
        if not ok:
            return []

        try:
            query_job = self.bq.query(sql)
            results = query_job.result()

            joins = []
            for row in results:
                join_info = {
                    "child_column": row.child_column,
                    "parent_table": f"{row.parent_project}.{row.parent_dataset}.{row.parent_table}",
                    "parent_column": row.parent_column,
                    "parent_schema": []
                }

                try:
                    parent_ref = bigquery.TableReference(
                        bigquery.DatasetReference(row.parent_project, row.parent_dataset),
                        row.parent_table
                    )
                    parent_bq_table = self.bq.get_table(parent_ref)
                    join_info["parent_schema"] = [
                        {
                            "name": f.name,
                            "type": f.field_type,
                            "description": f.description or ""
                        }
                        for f in parent_bq_table.schema
                    ]
                except Exception:
                    pass

                joins.append(join_info)

            return joins
        except Exception as e:
            print(f"Failed to fetch joins for {tgt.raw_id}: {e}")
            return []

    def _get_distinct_counts(self, tgt: TableTarget, schema) -> dict:
        """Quick scan to get distinct count per string column."""
        clauses = []
        names = []

        for f in schema:
            if f.field_type not in ('STRING',) or f.mode == 'REPEATED':
                continue
            clauses.append(f"COUNT(DISTINCT `{f.name}`) AS `{f.name}`")
            names.append(f.name)

        if not clauses:
            return {}

        sql = f"SELECT {', '.join(clauses)} FROM {tgt.fqtn}"

        ok, err, _ = self.dry_run(sql)
        if not ok:
            return {}

        try:
            rows = list(self.bq.query(sql).result())
            if not rows:
                return {}
            row = rows[0]
            return {name: getattr(row, name, None) for name in names}
        except Exception as e:
            print(f"Distinct count scan failed for {tgt.raw_id}: {e}")
            return {}

    def _get_sample_limit(self, field_name: str, field_type: str, distinct_count: int = None) -> int:
        """Determine sample limit based on data characteristics, not column names."""

        # Dates and timestamps — min/max is enough, no samples needed
        if field_type in ('DATE', 'DATETIME', 'TIMESTAMP', 'TIME'):
            return 0

        # Boolean — only true/false anyway
        if field_type == 'BOOL':
            return 2

        # Numeric — min/max/avg is enough, no samples needed
        if field_type in ('INT64', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC'):
            return 0

        # For strings: decide based on distinct count
        if distinct_count is not None:
            if distinct_count <= 50:
                return distinct_count   # low cardinality — show all (kpi, zone, status...)
            elif distinct_count <= 200:
                return 20               # medium cardinality — show a good sample
            else:
                return 5                # high cardinality — names, IDs, free text

        # Fallback if distinct count not available
        return 10

    def _get_column_profiles(self, tgt: TableTarget, schema) -> dict:
        """
        Builds a single optimized analytical query to extract:
        - Strings: dynamic sample limit based on distinct count
        - Bools: 2 samples
        - Numeric: Min, Max, Avg
        - Dates: Min, Max
        """

        # Pre-scan distinct counts for all string columns
        distinct_counts = self._get_distinct_counts(tgt, schema)

        select_clauses = []
        valid_fields = []

        for idx, f in enumerate(schema):
            if f.field_type in ('RECORD', 'STRUCT', 'ARRAY') or f.mode == 'REPEATED':
                continue

            fname = f"`{f.name}`"
            alias_base = f"c{idx}"

            if f.field_type in ('STRING', 'BOOL'):
                distinct = distinct_counts.get(f.name)
                limit = self._get_sample_limit(f.name, f.field_type, distinct)
                if limit > 0:
                    select_clauses.append(
                        f"ARRAY_AGG(DISTINCT CAST({fname} AS STRING) IGNORE NULLS LIMIT {limit}) AS `{alias_base}_samples`"
                    )
                valid_fields.append((f.name, 'categorical', alias_base, limit))

            elif f.field_type in ('INT64', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC'):
                select_clauses.append(f"CAST(ROUND(MIN({fname}), 0) AS STRING) AS `{alias_base}_min`")
                select_clauses.append(f"CAST(ROUND(MAX({fname}), 0) AS STRING) AS `{alias_base}_max`")
                select_clauses.append(f"CAST(ROUND(AVG({fname}), 0) AS STRING) AS `{alias_base}_avg`")
                valid_fields.append((f.name, 'numeric', alias_base, None))
                valid_fields.append((f.name, 'numeric', alias_base, None))

            elif f.field_type in ('DATE', 'DATETIME', 'TIMESTAMP', 'TIME'):
                select_clauses.append(f"CAST(MIN({fname}) AS STRING) AS `{alias_base}_min`")
                select_clauses.append(f"CAST(MAX({fname}) AS STRING) AS `{alias_base}_max`")
                valid_fields.append((f.name, 'datetime', alias_base, None))

        if not select_clauses:
            return {}

        sql = f"SELECT \n" + ",\n".join(select_clauses) + f"\nFROM {tgt.fqtn}"

        ok, err, _ = self.dry_run(sql)
        if not ok:
            print(f"Skipping profiling for {tgt.raw_id} due to dry-run failure: {err}")
            return {}

        try:
            rows = list(self.bq.query(sql).result())
            if not rows:
                return {}
            row = rows[0]

            profile_data = {}
            for fname, ftype, alias, limit in valid_fields:
                p = {}
                if ftype == 'categorical':
                    if limit and limit > 0:
                        samples = getattr(row, f"{alias}_samples", [])
                        p["sample_values"] = [str(s) for s in samples] if samples else []
                    else:
                        p["sample_values"] = []
                elif ftype == 'numeric':
                    p["min"] = getattr(row, f"{alias}_min", None)
                    p["max"] = getattr(row, f"{alias}_max", None)
                    p["avg"] = getattr(row, f"{alias}_avg", None)
                elif ftype == 'datetime':
                    p["min"] = getattr(row, f"{alias}_min", None)
                    p["max"] = getattr(row, f"{alias}_max", None)
                profile_data[fname] = p
            return profile_data

        except Exception as e:
            print(f"Profiling query failed for {tgt.raw_id}: {e}")
            return {}

    def build_table_card(self, tgt: TableTarget) -> Dict[str, Any]:
        t = self.get_table(tgt)

        profiles = self._get_column_profiles(tgt, t.schema)

        cols = []
        policy_tag_count = 0
        for f in t.schema:
            col_info = {
                "name": f.name,
                "type": f.field_type,
                "mode": f.mode,
                "description": f.description or ""
            }

            if f.name in profiles:
                col_info.update(profiles[f.name])

            cols.append(col_info)

            if getattr(f, "policy_tags", None) and getattr(f.policy_tags, "names", None):
                policy_tag_count += len(f.policy_tags.names)

        stats = {
            "total_rows": int(t.num_rows) if t.num_rows is not None else None,
            "bytes": int(t.num_bytes) if t.num_bytes is not None else None,
            "created": datetime.fromtimestamp(t.created.timestamp(), tz=timezone.utc).replace(microsecond=0).isoformat()
                        if getattr(t, "created", None) else None,
            "last_modified": datetime.fromtimestamp(t.modified.timestamp(), tz=timezone.utc).replace(microsecond=0).isoformat()
                        if getattr(t, "modified", None) else None,
            "partitioning": {
                "type": getattr(t.time_partitioning, "type_", None),
                "field": getattr(t.time_partitioning, "field", None),
            } if t.time_partitioning else None,
            "clustering_fields": list(t.clustering_fields) if t.clustering_fields else None,
        }

        return {
            "doc_id": tgt.raw_id,
            "project": tgt.project,
            "dataset": tgt.dataset,
            "table": tgt.table,
            "location": getattr(t, "location", None),
            "description": t.description or "",
            "columns": cols,
            "stats": stats,
            "governance": {"policy_tag_count": policy_tag_count},
            "join_relationships": self.get_join_relationships(tgt),
            "generated_at_utc": now_utc_iso(),
        }

    def infer_measures_dimensions(self, card: Dict[str, Any]) -> Dict[str, List[str]]:
        colnames = [c["name"].lower() for c in card.get("columns", [])]

        def has_any(substrs: List[str]) -> bool:
            return any(any(s in c for s in substrs) for c in colnames)

        measures, dims = [], []

        if has_any(["progress", "completion", "pct"]):
            measures += ["percent complete", "completion rate"]
        if has_any(["budget", "cost", "variance", "capex", "opex"]):
            measures += ["total cost", "budget variance", "actual spend", "capex"]
        if has_any(["hc", "headcount", "staff", "employee"]):
            measures += ["total headcount", "active employees"]
            dims += ["department", "grade", "nationality", "gender"]
        if has_any(["hired", "applicant", "vacancy", "recruitment"]):
            measures += ["hiring velocity", "time to fill", "open positions"]
        if has_any(["nps", "csat", "score", "rating"]):
            measures += ["customer satisfaction score", "net promoter score", "average rating"]
        if has_any(["wait", "queue", "dwell"]):
            measures += ["average wait time", "dwell time"]
        if has_any(["vendor", "supplier", "contractor"]):
            dims += ["vendor name", "supplier category"]
        if has_any(["po_", "invoice", "spend"]):
            measures += ["total spend", "procurement value"]
        if has_any(["ticket", "incident", "severity"]):
            measures += ["ticket volume", "resolution rate"]
            dims += ["priority", "issue type", "system"]

        measures = sorted(list(set([m.strip() for m in measures])))
        dims = sorted(list(set([d.strip() for d in dims])))

        return {"measures_inferred": measures, "dimensions_inferred": dims}

    def optional_profile(self, tgt: TableTarget, card: Dict[str, Any]) -> Dict[str, Any]:
        if not ENABLE_PROFILING:
            return {}

        cols = card.get("columns", [])
        date_cols = [c["name"] for c in cols if c["type"] in ("DATE", "DATETIME", "TIMESTAMP")]
        if not date_cols:
            return {}

        col = date_cols[0]
        sql = f"SELECT MIN({col}) AS min_v, MAX({col}) AS max_v FROM {tgt.fqtn}"
        ok, err, _ = self.dry_run(sql)
        return {"date_range_col": col, "date_range_dryrun_ok": ok, "date_range_error": "" if ok else err}


# =========================================================
# LLM Toolbox
# =========================================================
class LLMToolbox:
    def __init__(self, model_name: str):
        self.model = GenerativeModel(model_name)

    def prompt_for_table(self, card: Dict[str, Any]) -> str:
        fqtn = f"`{card['project']}.{card['dataset']}.{card['table']}`"

        col_lines_list = []
        for c in card["columns"]:
            line = f"- {c['name']} ({c['type']}): {c.get('description','')}"
            if c.get("sample_values"):
                line += f" [Samples: {', '.join(c['sample_values'])}]"
            if c.get("min") is not None:
                line += f" [Range: {c['min']} to {c['max']}]"
                if c.get("avg") is not None:
                    line += f" [Avg: {c['avg']}]"
            col_lines_list.append(line)
        col_lines = "\n".join(col_lines_list)

        joins_data = card.get("join_relationships", [])
        joins_context = json.dumps(joins_data, indent=2) if joins_data else "No explicit database foreign keys found."

        return f"""
You are a senior analytics engineer. Return STRICT JSON ONLY (no markdown, no commentary).

Table: {fqtn}
Use ONLY the columns listed below.

STRICT JSON KEYS (and ONLY these keys):
- measures: 5-30 business measure phrases + synonyms (e.g., "items sold", "units sold", "quantity sold")
- kpis: 10-15 KPI ideas (business phrasing)
- dimensions: 10-20 grouping dimensions (business phrasing)
- join_hints: 5-10 join/lineage hints (text only). Incorporate known explicit joins if provided below.
- sample_qa: at least 10 entries, each has:
  - question
  - sql (valid BigQuery Standard SQL using {fqtn})
  - answer_summary (ONE sentence)

Rules:
- Keep each SQL under 12 lines.
- Use BigQuery Standard SQL only.
- If explicit database joins are provided, utilize them to form accurate JOIN logic in sample_qa when applicable.
- Do not include any extra keys.

COLUMNS:
{col_lines}

KNOWN DATABASE JOINS:
{joins_context}
""".strip()

    def prompt_fix_sql(self, card: Dict[str, Any], bad_sql: str, error: str) -> str:
        fqtn = f"`{card['project']}.{card['dataset']}.{card['table']}`"
        col_names = [c["name"] for c in card["columns"]]
        return f"""
Fix this BigQuery SQL to pass dry-run.

Table: {fqtn}
Valid columns: {col_names}

Bad SQL:
{bad_sql}

Dry-run error:
{error}

Return STRICT JSON ONLY:
{{"sql":"<fixed_full_sql>"}}
""".strip()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def call_json(self, prompt: str) -> Dict[str, Any]:
        resp = self.model.generate_content(
            prompt,
            generation_config=GenerationConfig(
                temperature=0.2,
                max_output_tokens=8192,
                response_mime_type="application/json",
            ),
        )
        raw = (resp.text or "").strip()
        if not raw:
            raise ValueError("Gemini returned empty response")
        return json.loads(raw)

    @staticmethod
    def enforce_min_qa(enrichment: Dict[str, Any], min_n: int = 10) -> Dict[str, Any]:
        enrichment.setdefault("measures", [])
        enrichment.setdefault("kpis", [])
        enrichment.setdefault("dimensions", [])
        enrichment.setdefault("join_hints", [])
        enrichment.setdefault("sample_qa", [])
        if len(enrichment["sample_qa"]) < min_n:
            raise ValueError(f"LLM returned {len(enrichment['sample_qa'])} QAs; need >= {min_n}")
        return enrichment


# =========================================================
# Discovery Engine Toolbox
# =========================================================
class DiscoveryEngineToolbox:
    def __init__(self, location: str, data_store_id: str):
        self.loc = _sanitize_location(location)
        self.data_store_id = data_store_id

        if self.loc not in {"global", "us", "eu"}:
            raise ValueError(f"Invalid SEARCH_LOCATION='{location}'. Use one of: global, us, eu")

        if self.loc == "global":
            self.client = discoveryengine.DocumentServiceClient()
        else:
            self.client = discoveryengine.DocumentServiceClient(
                client_options=ClientOptions(api_endpoint=f"{self.loc}-discoveryengine.googleapis.com")
            )

    def import_documents(self, project_for_search: str, gcs_uri: str):
        if not self.data_store_id:
            raise ValueError("DATA_STORE_ID is required when REFRESH_SEARCH=true")

        parent = self.client.branch_path(
            project=project_for_search,
            location=self.loc,
            data_store=self.data_store_id,
            branch="default_branch",
        )

        mode = discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
        if RECONCILIATION_MODE.upper() == "FULL":
            mode = discoveryengine.ImportDocumentsRequest.ReconciliationMode.FULL

        request = discoveryengine.ImportDocumentsRequest(
            parent=parent,
            gcs_source=discoveryengine.GcsSource(
                input_uris=[gcs_uri],
                data_schema=DATA_SCHEMA,
            ),
            reconciliation_mode=mode,
        )

        op = self.client.import_documents(request=request)
        print(f"Refreshing Vertex Search datastore… op={op.operation.name}")
        op.result()
        print("✅ Search refresh completed.")


# =========================================================
# Search Text Builder
# =========================================================
def build_search_text(card: Dict[str, Any]) -> str:
    parts = []
    parts.append(f"Project: {card.get('project')}")
    parts.append(f"Dataset: {card.get('dataset')}")
    parts.append(f"Table: {card.get('table')}")
    parts.append(f"Description: {card.get('description','')}")
    parts.append("Columns:")

    for c in card.get("columns", []):
        base_line = f"- {c.get('name')} ({c.get('type')}): {c.get('description','')}"

        extras = []
        if c.get("sample_values"):
            extras.append(f"Samples: {', '.join(c['sample_values'])}")
        if c.get("min") is not None:
            if c.get("avg") is not None:
                extras.append(f"Range: [{c['min']} to {c['max']}], Avg: {c['avg']}")
            else:
                extras.append(f"Range: [{c['min']} to {c['max']}]")

        if extras:
            parts.append(f"{base_line} | {' | '.join(extras)}")
        else:
            parts.append(base_line)

    joins = card.get("join_relationships", [])
    if joins:
        parts.append("Explicit Database Joins (Foreign Keys):")
        for j in joins:
            parts.append(f"- Column {j.get('child_column')} joins to parent table {j.get('parent_table')} on column {j.get('parent_column')}.")

    inf = card.get("toolbox_inference", {})
    if inf:
        parts.append("Measures (inferred):")
        for m in inf.get("measures_inferred", []):
            parts.append(f"- {m}")
        parts.append("Dimensions (inferred):")
        for d in inf.get("dimensions_inferred", []):
            parts.append(f"- {d}")

    le = card.get("llm_enrichment", {})
    if le:
        parts.append("Measures (canonical + synonyms):")
        for m in le.get("measures", []):
            parts.append(f"- {m}")
        parts.append("Dimensions / group-by fields:")
        for d in le.get("dimensions", []):
            parts.append(f"- {d}")
        parts.append("KPIs / metrics ideas:")
        for k in le.get("kpis", []):
            parts.append(f"- {k}")
        parts.append("Join hints:")
        for j in le.get("join_hints", []):
            parts.append(f"- {j}")
        parts.append("Sample Q&A:")
        for qa in le.get("sample_qa", []):
            parts.append(f"- Q: {qa.get('question')}")
            parts.append(f"  SQL: {qa.get('sql')}")
            parts.append(f"  Answer: {qa.get('answer_summary')}")

    prof = card.get("profiling", {})
    if prof:
        parts.append("Profiling:")
        for k, v in prof.items():
            parts.append(f"- {k}: {v}")

    return "\n".join(parts)


# =========================================================
# GCS Uploader Toolbox
# =========================================================
class GCSToolbox:
    def __init__(self):
        self.client = storage.Client()

    def upload_jsonl(self, bucket_name: str, object_name: str, json_lines: List[str]) -> str:
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_string("\n".join(json_lines) + "\n", content_type="application/json")
        return f"gs://{bucket_name}/{object_name}"


# =========================================================
# Target resolver
# =========================================================
def resolve_targets(bqt: BigQueryToolbox) -> List[TableTarget]:
    targets: List[TableTarget] = []

    if BQ_TABLES_JSON:
        table_ids = parse_json_list(BQ_TABLES_JSON)
        for tid in table_ids:
            p, d, t = parse_table_id(tid)
            targets.append(TableTarget(p, d, t))
        return targets

    if not BQ_SOURCES_JSON:
        raise ValueError("Set either BQ_TABLES or BQ_SOURCES env var (JSON).")

    sources = parse_json_list(BQ_SOURCES_JSON)
    for s in sources:
        p = s["project"]
        d = s["dataset"]
        tables = bqt.list_tables(p, d, prefix=TABLE_NAME_PREFIX)
        for t in tables:
            targets.append(TableTarget(p, d, t))

    if MAX_TABLES > 0:
        targets = targets[:MAX_TABLES]

    return targets


# =========================================================
# MAIN
# =========================================================
def main():
    print(f"[{now_utc_iso()}] Starting…")

    bq = bigquery.Client()
    bqt = BigQueryToolbox(bq)
    gcs = GCSToolbox()

    targets = resolve_targets(bqt)
    print(f"Total tables to process: {len(targets)}")

    llm: Optional[LLMToolbox] = None
    if ENABLE_LLM:
        if not VERTEX_PROJECT:
            raise ValueError("VERTEX_PROJECT is required when ENABLE_LLM=true")
        vertexai.init(project=VERTEX_PROJECT, location=VERTEX_REGION)
        llm = LLMToolbox(GEMINI_MODEL)

    de: Optional[DiscoveryEngineToolbox] = None
    if REFRESH_SEARCH:
        de = DiscoveryEngineToolbox(SEARCH_LOCATION, DATA_STORE_ID)

    jsonl_lines: List[str] = []

    for i, tgt in enumerate(targets, start=1):
        print(f"({i}/{len(targets)}) {tgt.raw_id}")

        card = bqt.build_table_card(tgt)
        card["toolbox_inference"] = bqt.infer_measures_dimensions(card)
        card["profiling"] = bqt.optional_profile(tgt, card)

        if llm:
            try:
                enrichment = llm.call_json(llm.prompt_for_table(card))
                enrichment = llm.enforce_min_qa(enrichment, 10)

                for qa in enrichment["sample_qa"]:
                    sql = (qa.get("sql") or "").strip()
                    if not sql:
                        qa["dry_run_ok"] = False
                        qa["dry_run_error"] = "Missing SQL"
                        continue

                    if not DRY_RUN_VALIDATE:
                        qa["dry_run_ok"] = True
                        qa["dry_run_bytes"] = None
                        continue

                    ok, err, bytes_processed = bqt.dry_run(sql)
                    attempts = 0
                    while (not ok) and attempts < MAX_FIX_ATTEMPTS:
                        fix = llm.call_json(llm.prompt_fix_sql(card, sql, err))
                        sql = (fix.get("sql") or sql).strip()
                        ok, err, bytes_processed = bqt.dry_run(sql)
                        attempts += 1

                    qa["sql"] = sql
                    qa["dry_run_ok"] = ok
                    qa["dry_run_error"] = "" if ok else err
                    qa["dry_run_bytes"] = bytes_processed
                    time.sleep(LLM_SLEEP_SEC)

                card["llm_enrichment"] = enrichment
                card["llm_enrichment_error"] = ""
            except RetryError as re_err:
                inner = re_err.last_attempt.exception()
                card["llm_enrichment"] = {}
                card["llm_enrichment_error"] = f"{type(inner).__name__}: {inner}"
                card["llm_enrichment_traceback"] = "".join(
                    traceback.format_exception(type(inner), inner, inner.__traceback__)
                )[:4000]
            except Exception as e:
                card["llm_enrichment"] = {}
                card["llm_enrichment_error"] = f"{type(e).__name__}: {e}"
                card["llm_enrichment_traceback"] = "".join(
                    traceback.format_exception(type(e), e, e.__traceback__)
                )[:4000]

        card["search_text"] = build_search_text(card)

        safe_id = sanitize_id(card["doc_id"])

        structured_record = {
            "project": card["project"],
            "dataset": card["dataset"],
            "table": card["table"],
            "location": card.get("location") or "",
            "description": card.get("description") or "",
            "columns": card.get("columns") or [],
            "stats": card.get("stats") or {},
            "generated_at_utc": card.get("generated_at_utc") or "",
            "search_text": card.get("search_text") or "",
            "join_relationships_json": json.dumps(card.get("join_relationships") or [], ensure_ascii=False),
            "llm_enrichment_ok": bool(card.get("llm_enrichment")),
            "llm_enrichment_error": card.get("llm_enrichment_error") or "",
            "llm_enrichment_json": json.dumps(card.get("llm_enrichment") or {}, ensure_ascii=False),
            "toolbox_inference_json": json.dumps(card.get("toolbox_inference") or {}, ensure_ascii=False),
            "profiling_json": json.dumps(card.get("profiling") or {}, ensure_ascii=False),
            "keyword_boost": build_keyword_boost(card.get("llm_enrichment") or {}),
        }

        doc = {
            "id": safe_id,
            "jsonData": json.dumps(structured_record, ensure_ascii=False)
        }

        jsonl_lines.append(json.dumps(doc, ensure_ascii=False))

    object_name = f"{GCS_PREFIX}/bq_schema_cards_{run_ts()}.jsonl"
    gcs_uri = gcs.upload_jsonl(GCS_BUCKET, object_name, jsonl_lines)
    print(f"✅ Uploaded JSONL: {gcs_uri}")

    if de:
        search_project = VERTEX_PROJECT or targets[0].project
        de.import_documents(search_project, gcs_uri)

    print(f"[{now_utc_iso()}] Done.")


if __name__ == "__main__":
    main()