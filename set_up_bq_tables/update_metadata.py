import json
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

# ==========================================
# SETUP
# ==========================================
PROJECT_ID = "prj-ai-dev-qic"
client = bigquery.Client(project=PROJECT_ID)

# Load manifest
with open("manifest.json", "r") as f:
    manifest = json.load(f)

print("\n--- 🚀 Pushing Metadata, PKs, and FKs to BigQuery ---")

# ==========================================
# 1️⃣ UPDATE DESCRIPTIONS
# ==========================================
for tb_info in manifest.get("tables", []):
    dataset_id = tb_info["dataset_id"]
    table_id = tb_info["table_id"]
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    try:
        table = client.get_table(table_ref)
        table.description = tb_info.get("description", "")

        new_schema = []
        for bq_col in table.schema:
            json_col = next(
                (c for c in tb_info.get("columns", []) if c["name"] == bq_col.name),
                None
            )
            desc = json_col["description"] if json_col else bq_col.description

            new_schema.append(
                bigquery.SchemaField(
                    name=bq_col.name,
                    field_type=bq_col.field_type,
                    mode=bq_col.mode,
                    description=desc
                )
            )

        table.schema = new_schema
        client.update_table(table, ["description", "schema"])
        print(f"✅ Updated descriptions: {table_id}")

    except Exception as e:
        print(f"❌ Description update failed for {table_id}: {e}")

# ==========================================
# 2️⃣ ADD PRIMARY KEYS (FIRST PASS)
# ==========================================
print("\n--- 🔑 Adding Primary Keys ---")

for tb_info in manifest.get("tables", []):
    dataset_id = tb_info["dataset_id"]
    table_id = tb_info["table_id"]
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    pks = tb_info.get("primary_key", [])
    if not pks:
        continue

    pk_str = ", ".join(pks)
    ddl_pk = f"""
    ALTER TABLE `{table_ref}`
    ADD PRIMARY KEY ({pk_str}) NOT ENFORCED
    """

    try:
        client.query(ddl_pk).result()
        print(f"🔑 PK added on {table_id}: ({pk_str})")
    except BadRequest as e:
        msg = str(e).lower()
        if "already has a primary key" in msg or "already exists" in msg:
            print(f"🔑 PK already exists on {table_id}")
        else:
            print(f"❌ PK error on {table_id}: {e}")

# ==========================================
# 3️⃣ ADD FOREIGN KEYS (SECOND PASS)
# ==========================================
print("\n--- 🔗 Adding Foreign Keys ---")

for tb_info in manifest.get("tables", []):
    dataset_id = tb_info["dataset_id"]
    table_id = tb_info["table_id"]
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    fks = tb_info.get("foreign_keys", [])
    for fk in fks:
        fk_cols = ", ".join(fk["columns"])
        ref_table = f"{PROJECT_ID}.{fk['ref_table']}"
        ref_cols = ", ".join(fk["ref_columns"])

        ddl_fk = f"""
        ALTER TABLE `{table_ref}`
        ADD FOREIGN KEY ({fk_cols})
        REFERENCES `{ref_table}`({ref_cols})
        NOT ENFORCED
        """

        try:
            client.query(ddl_fk).result()
            print(f"🔗 FK added: {table_id}({fk_cols}) → {ref_table}")
        except BadRequest as e:
            msg = str(e).lower()
            if "already exists" in msg:
                print(f"🔗 FK already exists on {table_id}({fk_cols})")
            elif "does not have primary key" in msg:
                print(f"❌ FK failed: Parent table missing PK for {ref_table}")
            else:
                print(f"❌ FK error on {table_id}: {e}")

print("\n🎉 All metadata and constraints processed successfully!")