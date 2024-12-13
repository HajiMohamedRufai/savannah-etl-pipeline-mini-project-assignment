from google.cloud import bigquery
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
            
# BigQuery queries
user_summary_query = """
CREATE OR REPLACE TABLE savannah_etl.user_summary AS
SELECT
    u.user_id,
    u.first_name,
    SUM(c.total_cart_value) AS total_spent,
    SUM(c.quantity) AS total_items,
    u.age,
    u.city
FROM
    `savannah-etl.savannah_etl.users_table` AS u
JOIN
    `savannah-etl.savannah_etl.carts_table` AS c
ON
    u.user_id = c.user_id
GROUP BY
    u.user_id, u.first_name, u.age, u.city;
"""

cart_details_query = """
CREATE OR REPLACE TABLE savannah_etl.cart_details AS
SELECT
    c.cart_id,
    c.user_id,
    c.product_id,
    c.quantity,
    p.price,
    (c.quantity * p.price) AS total_cart_value
FROM
    `savannah-etl.savannah_etl.carts_table` AS c
JOIN
    `savannah-etl.savannah_etl.products_table` AS p
ON
    c.product_id = p.product_id;
"""

category_summary_query = """
CREATE OR REPLACE TABLE savannah_etl.category_summary AS
SELECT
    p.category,
    SUM(c.quantity * p.price) AS total_sales,
    SUM(c.quantity) AS total_items_sold
FROM
    `savannah-etl.savannah_etl.carts_table` AS c
JOIN
    `savannah-etl.savannah_etl.products_table` AS p
ON
    c.product_id = p.product_id
GROUP BY
    p.category;
"""

