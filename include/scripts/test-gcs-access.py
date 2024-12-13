from google.cloud import storage

def test_gcs_auth():
    client = storage.Client()
    buckets = list(client.list_buckets())
    print("Buckets:", buckets)

test_gcs_auth()
