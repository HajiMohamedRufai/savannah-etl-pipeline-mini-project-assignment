import requests
import pandas as pd

# URL of the JSON data
url = "https://dummyjson.com/products"

# Fetch the JSON data
response = requests.get(url)
if response.status_code == 200:
    print("response.status_code == 200")
    data = response.json()
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    data = {}

# Extract relevant fields and filter products
filtered_products = []
for product in data.get("products", []):
    if product.get("price", 0) > 50:  # Exclude products with price <= 50
        filtered_products.append({
            "product_id": product.get("id"),
            "name": product.get("title"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "price": product.get("price"),
        })

# Create a Pandas DataFrame
products_df = pd.DataFrame(filtered_products)

# Save the DataFrame to a CSV file
csv_file = "filtered_products.csv"
products_df.to_csv(csv_file, index=False)

print(f"Filtered products saved to {csv_file}")
