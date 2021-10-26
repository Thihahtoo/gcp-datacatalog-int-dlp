import json
from google.cloud import storage

def list_file_gcs(project_id, bucketname, prefix):
    storage_client = storage.Client(project=project_id)
    blobs = storage_client.list_blobs(bucketname, prefix=prefix)
    file_list = []
    for blob in blobs:
        file_list.append(blob.name)
    return file_list

def read_json_gcs(project_id, bucketname, filename):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucketname)
    template = bucket.blob(filename)
    template = json.loads(template.download_as_string())
    return template

def move_file_gcs(project_id, bucketname, blobname, destination_bucket_name, destination_blob_name):
    storage_client = storage.Client(project=project_id)
    source_bucket = storage_client.bucket(bucketname)
    source_blob = source_bucket.blob(blobname)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_bucket.delete_blob(blobname)

def download_file_gcs(project_id, bucketname, blobname, destination):
    storage_client = storage.Client(project=project_id)
    source_bucket = storage_client.bucket(bucketname)
    blob = source_bucket.blob(blobname)
    blob.download_to_filename(destination)