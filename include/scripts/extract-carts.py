import requests
import pandas as pd

# URL of the JSON data
url = "https://dummyjson.com/carts"

# Fetch the JSON data
response = requests.get(url)
if response.status_code == 200:
    print("response.status_code == 200")
    data = response.json()
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    data = {}

# Extract and process the carts data
flattened_data = []
for cart in data.get("carts", []):
    total_cart_value = sum(product.get("total", 0) for product in cart.get("products", []))
    for product in cart.get("products", []):
        flattened_data.append({
            "cart_id": cart.get("id"),
            "user_id": cart.get("userId", None),  # Assuming userId field might exist
            "product_id": product.get("id"),
            "quantity": product.get("quantity"),
            "price": product.get("price"),
            "total_cart_value": total_cart_value,
        })

# Create a Pandas DataFrame
carts_df = pd.DataFrame(flattened_data)

# Save the DataFrame to a CSV file
csv_file = "flattened_carts.csv"
carts_df.to_csv(csv_file, index=False)

print(f"Flattened cart data saved to {csv_file}")