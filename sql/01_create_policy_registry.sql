CREATE TABLE IF NOT EXISTS `governance.table_policy_registry` (
  dataset_id STRING,
  table_id STRING,
  classification STRING,
  pii_flag BOOL,
  allow_sql BOOL,
  allow_agg_only BOOL,
  allow_synthesis BOOL,
  join_group STRING,
  allow_cross_group_join BOOL,
  owner_team STRING,
  approved_by STRING,
  notes STRING,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP
);
