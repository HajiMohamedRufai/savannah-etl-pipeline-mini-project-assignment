# Import necessary libraries and modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.extract_function import fetch_and_save_to_gcs  # extract
from include.transformation_functions import transform_data, transform_users, transform_products, transform_carts  # transform
from include.load_function import users_schema, products_schema, carts_schema, load_csv_to_bigquery  # load
from include.helper_functions import create_dataset_if_not_exists, user_summary_query, category_summary_query, cart_details_query
from google.cloud import bigquery
from airflow.operators.dummy import DummyOperator
from google.api_core.exceptions import NotFound
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    # Default arguments for the DAG
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'savannah_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for savannah-etl project',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    """
    DAG for the savannah ETL pipeline that orchestrates the extraction, transformation,
    and loading of data from APIs into Google BigQuery.
    """

    # DummyOperator tasks to denote the start and end of the pipeline
    start_task = DummyOperator(task_id='start')
    loaded_into_bigquery = DummyOperator(task_id='loaded_into_bigquery')

    # Task to create BigQuery dataset if it doesn't exist
    create_dataset_task = PythonOperator(
        task_id="create_dataset",
        python_callable=create_dataset_if_not_exists,
        op_kwargs={"dataset_name": "savannah_etl", "location": "US"},
    )

    # Extract tasks to fetch data from APIs and store in GCS
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

    # Transform tasks to process the JSON data and convert it to CSV
    transform_users_task = PythonOperator(
        task_id='transform_users',
        python_callable=transform_data,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'users.json',
            'destination_file': 'transformed_users.csv',
            'transform_func': transform_users,
        },
    )

    transform_products_task = PythonOperator(
        task_id='transform_products',
        python_callable=transform_data,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'products.json',
            'destination_file': 'transformed_products.csv',
            'transform_func': transform_products,
        },
    )

    transform_carts_task = PythonOperator(
        task_id='transform_carts',
        python_callable=transform_data,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'carts.json',
            'destination_file': 'transformed_carts.csv',
            'transform_func': transform_carts,
        },
    )

    # Load tasks to upload CSV files to BigQuery tables
    load_users_task = PythonOperator(
        task_id='load_users',
        python_callable=load_csv_to_bigquery,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'transformed_users.csv',
            'dataset_name': 'savannah_etl',
            'table_name': 'users_table',
            'schema': users_schema,
        },
    )

    load_products_task = PythonOperator(
        task_id='load_products',
        python_callable=load_csv_to_bigquery,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'transformed_products.csv',
            'dataset_name': 'savannah_etl',
            'table_name': 'products_table',
            'schema': products_schema,
        },
    )

    load_carts_task = PythonOperator(
        task_id='load_carts',
        python_callable=load_csv_to_bigquery,
        op_kwargs={
            'bucket_name': 'savannah-etl-bucket',
            'source_file': 'transformed_carts.csv',
            'dataset_name': 'savannah_etl',
            'table_name': 'carts_table',
            'schema': carts_schema,
        },
    )

    # Define task dependencies to establish the order of execution
    start_task >> create_dataset_task
    create_dataset_task >> [extract_users, extract_products, extract_carts]
    
    extract_users >> transform_users_task  
    extract_products >> transform_products_task
    extract_carts >> transform_carts_task

    transform_users_task >> load_users_task
    transform_products_task >> load_products_task
    transform_carts_task >> load_carts_task
    
    [load_users_task, load_products_task, load_carts_task] >> loaded_into_bigquery

