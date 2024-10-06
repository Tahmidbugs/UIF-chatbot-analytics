from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict

# Define your service account credentials and project ID
credentials = service_account.Credentials.from_service_account_file('./service-account-key.json')
project_id = 'blackbyrdtest'
client = bigquery.Client(credentials=credentials, project=project_id)

# Define BigQuery dataset and table schemas
dataset_id = 'chatanalytics'  # Replace with your actual dataset name

tables_schemas = {
    
    "sessions_table": [
        bigquery.SchemaField("count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED")
    ],
    
    "conversation_summary": [
        bigquery.SchemaField("conversation_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("conversation_length", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("chatbot_total_length", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("user_total_length", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("rating", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("self_serviced", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("sharia_terms_used", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("nlu_errors", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("token_quota_exceeded_count", "INTEGER", mode="REQUIRED"),
    ],
    "chatbot_didnt_understand_table" : [
    bigquery.SchemaField("query", "STRING", mode="REQUIRED"), 
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"), 
    ],    
    "top_intents_table" : [
    bigquery.SchemaField("intent", "STRING", mode="REQUIRED"),  # Intent name
    bigquery.SchemaField("count", "INTEGER", mode="REQUIRED"),  # Count of occurrences
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED")  # Insertion timestamp
    ],
}

# Create tables in the specified dataset
for table_name, schema in tables_schemas.items():
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)
    try:
        table = client.create_table(table)  # Make an API request.
        print(f"Created table {table.table_id}")
    except Conflict:
        print(f"Table {table_id} already exists. Skipping creation.")
    except Exception as e:
        print(f"An error occurred while creating table {table_id}: {e}")
