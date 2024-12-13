from google.cloud import bigquery
import os

# Define BigQuery schema
users_schema = [
    bigquery.SchemaField("user_id", "INTEGER"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("street", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("postal_code", "STRING"),
]

products_schema = [
    bigquery.SchemaField("product_id", "INTEGER"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("price", "FLOAT"),
]

carts_schema = [
    bigquery.SchemaField("cart_id", "INTEGER"),
    bigquery.SchemaField("user_id", "INTEGER"),
    bigquery.SchemaField("product_id", "INTEGER"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("total_cart_value", "FLOAT"),
    ]

def load_csv_to_bigquery(bucket_name, source_file, dataset_name, table_name, schema):
    """
    Load a CSV file from GCS into a BigQuery table.
    """
    client = bigquery.Client()
    table_id = f"{client.project}.{dataset_name}.{table_name}"

    # Define GCS URI for the file
    gcs_uri = f"gs://{bucket_name}/{source_file}"

    # Define BigQuery job configuration
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace table content
    )

    # Load data into BigQuery
    load_job = client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    load_job.result()  # Wait for the job to complete

    print(f"Loaded {source_file} into {table_id}")
