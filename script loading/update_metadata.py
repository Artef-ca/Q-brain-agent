import json
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

# ==========================================
# SETUP
# ==========================================
PROJECT_ID = "prj-ai-dev-qic" # Ensure this is your GCP Project ID
client = bigquery.Client(project=PROJECT_ID)

# Load the JSON manifest
with open("manifest.json", "r") as f:
    manifest = json.load(f)

print("--- Pushing Metadata, PKs, and FKs to BigQuery ---")

for tb_info in manifest.get("tables", []):
    dataset_id = tb_info["dataset_id"]
    table_id = tb_info["table_id"]
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    
    # ---------------------------------------------------------
    # 1. Update Table & Column Descriptions
    # ---------------------------------------------------------
    try:
        table = client.get_table(table_ref)
        table.description = tb_info.get("description", "")

        new_schema = []
        for bq_col in table.schema:
            # Find the matching column description in the JSON
            json_col = next((c for c in tb_info.get("columns", []) if c["name"] == bq_col.name), None)
            desc = json_col["description"] if json_col else bq_col.description
            
            # Rebuild the schema field with the description attached
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
        print(f"\n✅ Updated descriptions for: {table_id}")
        
    except Exception as e:
        print(f"\n❌ Could not update descriptions for {table_id}: {e}")
        continue # Skip constraints if table doesn't exist

    # ---------------------------------------------------------
    # 2. Add Primary Key Constraints (NOT ENFORCED)
    # ---------------------------------------------------------
    pks = tb_info.get("primary_key", [])
    if pks:
        pk_str = ", ".join(pks)
        ddl_pk = f"ALTER TABLE `{table_ref}` ADD PRIMARY KEY ({pk_str}) NOT ENFORCED;"
        try:
            client.query(ddl_pk).result()
            print(f"  🔑 Added PK: ({pk_str})")
        except BadRequest as e:
            if "already has a primary key" in str(e):
                print(f"  🔑 PK already exists: ({pk_str})")
            else:
                print(f"  ❌ PK Error: {e}")

    # ---------------------------------------------------------
    # 3. Add Foreign Key Constraints (NOT ENFORCED)
    # ---------------------------------------------------------
    fks = tb_info.get("foreign_keys", [])
    for fk in fks:
        fk_cols = ", ".join(fk["columns"])
        ref_table = f"{PROJECT_ID}.{fk['ref_table']}"
        ref_cols = ", ".join(fk["ref_columns"])
        
        # Build a constraint name to avoid duplicate errors
        constraint_name = f"fk_{table_id}_{fk['columns'][0]}".lower()
        
        ddl_fk = f"""
        ALTER TABLE `{table_ref}` 
        ADD CONSTRAINT `{constraint_name}` 
        FOREIGN KEY ({fk_cols}) REFERENCES `{ref_table}`({ref_cols}) NOT ENFORCED;
        """
        try:
            client.query(ddl_fk).result()
            print(f"  🔗 Added FK: ({fk_cols}) -> {ref_table}")
        except BadRequest as e:
            if "already exists" in str(e):
                print(f"  🔗 FK already exists: ({fk_cols})")
            else:
                print(f"  ❌ FK Error: {e}")

print("\n🎉 Schema metadata and constraints successfully pushed!")