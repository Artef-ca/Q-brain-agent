CREATE TABLE IF NOT EXISTS `agent_observability.traces` (
  trace_id STRING,
  session_id STRING,
  user_id STRING,
  status STRING,
  final_intent STRING,
  answer_len INT64,
  latency_ms INT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `agent_observability.routing_decisions` (
  trace_id STRING,
  step INT64,
  agent STRING,
  decision_type STRING,
  payload JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `agent_observability.tool_calls` (
  trace_id STRING,
  tool_name STRING,
  input JSON,
  output JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `agent_observability.sql_audit` (
  trace_id STRING,
  sql_text STRING,
  tables_referenced ARRAY<STRING>,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `agent_observability.rag_citations` (
  trace_id STRING,
  source JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
