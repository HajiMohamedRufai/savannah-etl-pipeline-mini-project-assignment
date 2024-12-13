from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os
from google.cloud import storage
from include.fetch_and_save_to_gcs import fetch_and_save_to_gcs
from include.transformation_functions import transform_users, transform_products, transform_carts
import pandas as pd
from google.cloud import storage
import json



# Set default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
with DAG(
    'savannah_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for savannah-etl project',
    schedule_interval='@daily',  # Adjust based on your requirements
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    

    # Define tasks
    extract_users = PythonOperator(
        task_id='extract_users',
        python_callable=fetch_and_save_to_gcs,
        op_args=['https://dummyjson.com/users', 'savannah-etl-bucket', 'users.json'],
    )

    extract_products = PythonOperator(
        task_id='extract_products',
        python_callable=fetch_and_save_to_gcs,
        op_args=['https://dummyjson.com/products', 'savannah-etl-bucket', 'products.json'],
    )

    extract_carts = PythonOperator(
        task_id='extract_carts',
        python_callable=fetch_and_save_to_gcs,
        op_args=['https://dummyjson.com/carts', 'savannah-etl-bucket', 'carts.json'],
    )

    # Set task dependencies
    [extract_users, extract_products, extract_carts]
    
    # PART 2
    # Function to read from GCS, transform data into a DataFrame, and save it back as CSV
    def transform_data(bucket_name, source_file, destination_file, transform_func):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Read JSON from GCS
        blob = bucket.blob(source_file)
        data = json.loads(blob.download_as_text())
        
        # Apply transformation to create a DataFrame
        df = transform_func(data)
        
        # Save DataFrame to CSV locally
        local_csv_path = f'/tmp/{destination_file}'
        df.to_csv(local_csv_path, index=False)
        print(f"Transformed data saved to {local_csv_path}")
        
        # Upload transformed CSV back to GCS
        blob = bucket.blob(destination_file)
        blob.upload_from_filename(local_csv_path)
        print(f"Uploaded {destination_file} to GCS bucket {bucket_name}")

    # Define tasks
    transform_users_task = PythonOperator(
        task_id='transform_users',
        python_callable=transform_data,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'users.json',
            'destination_file': 'transformed_users.json',
            'transform_func': transform_users,
        }
    )

    transform_products_task = PythonOperator(
        task_id='transform_products',
        python_callable=transform_data,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'products.json',
            'destination_file': 'transformed_products.json',
            'transform_func': transform_products,
        }
    )

    transform_carts_task = PythonOperator(
        task_id='transform_carts',
        python_callable=transform_data,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'carts.json',
            'destination_file': 'transformed_carts.json',
            'transform_func': transform_carts,
        }
    )

    # Task dependencies
    extract_users >> transform_users_task
    extract_products >> transform_products_task
    extract_carts >> transform_carts_task
