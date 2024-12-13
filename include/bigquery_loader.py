from google.cloud import bigquery
import os

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
