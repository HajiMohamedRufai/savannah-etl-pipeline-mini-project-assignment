import requests
import pandas as pd

# URL for the JSON data
url = "https://dummyjson.com/users"

# Fetch the JSON data
response = requests.get(url)
if response.status_code == 200:
    print("response.status_code == 200")
    data = response.json()
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    data = {}

# Extract relevant fields and store them in a list of dictionaries
users_data = []
if "users" in data:
    for user in data["users"]:
        extracted_user = {
            "user_id": user.get("id"),
            "first_name": user.get("firstName"),
            "last_name": user.get("lastName"),
            "gender": user.get("gender"),
            "age": user.get("age"),
            "street": user.get("address", {}).get("address"),
            "city": user.get("address", {}).get("city"),
            "postal_code": user.get("address", {}).get("postalCode"),
        }
        users_data.append(extracted_user)

# Create a pandas DataFrame
df = pd.DataFrame(users_data)

# Save the DataFrame to a CSV file
csv_file = "users_data.csv"
df.to_csv(csv_file, index=False)

print(f"Data successfully saved to {csv_file}")


