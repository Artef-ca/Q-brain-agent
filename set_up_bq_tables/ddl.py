import json
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

def create_schema_from_manifest(manifest_path: str, project_id: str = None):
    """
    Reads a JSON manifest and creates/replaces BigQuery datasets and tables.
    """
    # Initialize the BigQuery client
    # If project_id is None, it uses the default project from your environment
    client = bigquery.Client(project=project_id)
    
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    default_location = manifest.get("default_location", "EU")

    # 1. Create Datasets
    print("--- Creating Datasets ---")
    for ds_info in manifest.get("datasets", []):
        dataset_id = f"{client.project}.{ds_info['dataset_id']}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.description = ds_info.get("description", "")
        dataset.location = default_location

        try:
            # exists_ok=True prevents errors if the dataset is already there
            dataset = client.create_dataset(dataset, exists_ok=True)
            print(f"✅ Dataset ready: {dataset_id}")
        except Exception as e:
            print(f"❌ Error creating dataset {dataset_id}: {e}")

    # 2. Create Tables
    print("\n--- Creating / Replacing Tables ---")
    for tb_info in manifest.get("tables", []):
        table_ref = f"{client.project}.{tb_info['dataset_id']}.{tb_info['table_id']}"
        
        # Build the Schema
        schema = []
        for col in tb_info.get("columns", []):
            schema.append(
                bigquery.SchemaField(
                    name=col["name"],
                    field_type=col["type"],
                    mode=col["mode"],
                    description=col.get("description", "")
                )
            )

        table = bigquery.Table(table_ref, schema=schema)
        table.description = tb_info.get("description", "")

        # Handle Partitioning
        partition_info = tb_info.get("partitioning")
        if partition_info:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=partition_info["type"],
                field=partition_info["field"]
            )

        # Handle Clustering
        clustering_info = tb_info.get("clustering_fields")
        if clustering_info:
            table.clustering_fields = clustering_info

        # Create or Replace the Table
        try:
            client.create_table(table)
            print(f"✅ Created new table: {table_ref}")
        except Conflict:
            print(f"⚠️ Table exists. Replacing: {table_ref}...")
            try:
                client.delete_table(table_ref)
                client.create_table(table)
                print(f"🔄 Successfully replaced: {table_ref}")
            except Exception as e:
                print(f"❌ Error replacing table {table_ref}: {e}")
        except Exception as e:
            print(f"❌ Error creating table {table_ref}: {e}")

if __name__ == "__main__":
    # If your environment is set up with a default project, you can leave project_id=None
    # Otherwise, specify your GCP project ID here: project_id="your-gcp-project-id"
    create_schema_from_manifest("manifest.json")