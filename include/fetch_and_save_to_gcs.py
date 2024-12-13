import requests
import json
from google.cloud import storage

def fetch_and_save_to_gcs(endpoint, bucket_name, filename):
    """
    Fetch data from an API endpoint and save it as JSON to Google Cloud Storage.
    """
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        local_path = f'/tmp/{filename}'
        
        # Save JSON locally
        with open(local_path, 'w') as f:
            json.dump(data, f)
        print(f"JSON Data saved locally to {local_path}")
        
        # Upload JSON to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.upload_from_filename(local_path)
        print(f"Uploaded {filename} to GCS bucket {bucket_name}")
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")
