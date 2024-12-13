import pandas as pd
import json
from google.cloud import storage

def transform_data(bucket_name, source_file, destination_file, transform_func):
    """
    Read JSON from GCS, transform it using the provided function, and save it as CSV back to GCS.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Read JSON from GCS
    blob = bucket.blob(source_file)
    data = json.loads(blob.download_as_text())
    
    # Transform data into DataFrame
    df = transform_func(data)
    
    # Save DataFrame to CSV locally
    local_csv_path = f'/tmp/{destination_file}'
    df.to_csv(local_csv_path, index=False)
    print(f"Transformed data saved to {local_csv_path}")
    
    # Upload CSV to GCS
    blob = bucket.blob(destination_file)
    blob.upload_from_filename(local_csv_path)
    print(f"Uploaded {destination_file} to GCS bucket {bucket_name}")

def transform_users(data):
    users_data = [
        {
            "user_id": user.get("id"),
            "first_name": user.get("firstName"),
            "last_name": user.get("lastName"),
            "gender": user.get("gender"),
            "age": user.get("age"),
            "street": user.get("address", {}).get("address"),
            "city": user.get("address", {}).get("city"),
            "postal_code": user.get("address", {}).get("postalCode"),
        }
        for user in data.get("users", [])
    ]
    return pd.DataFrame(users_data)

def transform_products(data):
    filtered_products = [
        {
            "product_id": product.get("id"),
            "name": product.get("title"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "price": product.get("price"),
        }
        for product in data.get("products", [])
        if product.get("price", 0) > 50
    ]
    return pd.DataFrame(filtered_products)

def transform_carts(data):
    flattened_data = []
    for cart in data.get("carts", []):
        total_cart_value = sum(product.get("price", 0) * product.get("quantity", 0) for product in cart.get("products", []))
        for product in cart.get("products", []):
            flattened_data.append({
                "cart_id": cart.get("id"),
                "user_id": cart.get("userId"),
                "product_id": product.get("id"),
                "quantity": product.get("quantity"),
                "price": product.get("price"),
                "total_cart_value": total_cart_value,
            })
    return pd.DataFrame(flattened_data)
