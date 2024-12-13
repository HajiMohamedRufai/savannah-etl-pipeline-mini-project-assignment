from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.fetch_and_save_to_gcs import fetch_and_save_to_gcs
from include.transformation_functions import transform_data, transform_users, transform_products, transform_carts
from include.bigquery_loader import load_csv_to_bigquery
from google.cloud import bigquery
from airflow.operators.dummy import DummyOperator
from google.api_core.exceptions import NotFound


default_args = {
    'owner': 'airflow',
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

    # Dummy start and end tasks
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')
    
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

    # Load tasks
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
     
    
    def create_dataset_if_not_exists(dataset_name, location="EU"):
        """
        Create a BigQuery dataset if it doesn't already exist.
        """
        client = bigquery.Client()
        dataset_id = f"{client.project}.{dataset_name}"

        try:
            client.get_dataset(dataset_id)  # Check if dataset exists
            print(f"Dataset {dataset_id} already exists.")
        except NotFound:
            # Create the dataset
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = location
            client.create_dataset(dataset)
            print(f"Created dataset {dataset_id}.")
        
        
    create_dataset_task = PythonOperator(
        task_id="create_dataset",
        python_callable=create_dataset_if_not_exists,
        op_kwargs={"dataset_name": "savannah_etl", "location": "US"},
        )

    # # Task Dependencies
    # extract_users >> transform_users_task  
    # extract_products >> transform_products_task
    # extract_carts >> transform_carts_task
 
    # # Update task dependencies
    # transform_users_task >> load_users_task
    # transform_products_task >> load_products_task
    # transform_carts_task >> load_carts_task
    # create_dataset_task >> [load_users_task, load_products_task, load_carts_task]

    # Task Dependencies
    start_task >> create_dataset_task
    create_dataset_task >> [extract_users, extract_products, extract_carts]
    
    extract_users >> transform_users_task  
    extract_products >> transform_products_task
    extract_carts >> transform_carts_task

    transform_users_task >> load_users_task
    transform_products_task >> load_products_task
    transform_carts_task >> load_carts_task
    
    [load_users_task, load_products_task, load_carts_task] >> end_task 