import pandas as pd
from google.cloud import storage
import json



# Transformation for Users
def transform_users(data):
    users_data = []
    for user in data.get("users", []):
        users_data.append({
            "user_id": user.get("id"),
            "first_name": user.get("firstName"),
            "last_name": user.get("lastName"),
            "gender": user.get("gender"),
            "age": user.get("age"),
            "street": user.get("address", {}).get("address"),
            "city": user.get("address", {}).get("city"),
            "postal_code": user.get("address", {}).get("postalCode"),
        })
    return pd.DataFrame(users_data)

# Transformation for Products
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
        if product.get("price", 0) > 50  # Filter out products with price <= 50
    ]
    return pd.DataFrame(filtered_products)

# Transformation for Carts
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
