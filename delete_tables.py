from google.cloud import bigquery
from google.oauth2 import service_account

# Define your service account credentials and project ID
credentials = service_account.Credentials.from_service_account_file('./service-account-key.json')
project_id = 'blackbyrdtest'
client = bigquery.Client(credentials=credentials, project=project_id)

# Define BigQuery dataset and table names
dataset_id = 'chatanalytics'  # Replace with your actual dataset name

tables = ["sessions_table", "conversation_summary", "chatbot_didnt_understand_table", "top_intents_table"]

def empty_tables(tables):
    for table_name in tables:
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        query = f"DELETE FROM `{table_id}` WHERE TRUE"  # Deletes all rows
        try:
            # Run the query to delete all rows from the table
            query_job = client.query(query)
            query_job.result()  # Wait for the job to complete
            print(f"Successfully emptied table: {table_name}")
        except Exception as e:
            print(f"An error occurred while emptying table {table_name}: {e}")

# Call the function to empty out the tables
empty_tables(tables)
