import json
from google.cloud import bigquery

PROJECT_ID = "smartadvisor-483817"
client = bigquery.Client(project=PROJECT_ID)

# Load your original manifest
with open("manifest.json", 'r') as f:
    manifest = json.load(f)

expected_tables = [f"{t['dataset_id']}.{t['table_id']}" for t in manifest["tables"]]
found_tables = []

# Fetch what's actually in BigQuery
datasets = list(client.list_datasets())
for ds in datasets:
    tables = list(client.list_tables(ds.reference))
    for t in tables:
        found_tables.append(f"{ds.dataset_id}.{t.table_id}")

# Compare
missing = set(expected_tables) - set(found_tables)

print(f"Expected: {len(expected_tables)} tables")
print(f"Found in BQ: {len(found_tables)} tables")
print(f"\nMissing Tables:")
for m in missing:
    print(f"- {m}")