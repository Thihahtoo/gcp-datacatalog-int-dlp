from google.cloud import storage
from google.cloud import bigquery
from utils.utils import run_shell_cmd , read_json
import os
import shutil

######### ################## ################## #########
######### ########### getting config values
######### ################## ################## #########
job_config = read_json("config/config.json")
resource_location = job_config["resource_location"]
project_id = job_config["project_id"]
extract_bucket = job_config["extract_bucket"]
template_landing_bucket = job_config["template_landing_bucket"]
template_archive_bucket = job_config["template_archive_bucket"]

reporting_dataset = job_config["extract_destination_dataset"]
tmpl_destination_table = job_config["template_extract_destination_table"]
tag_destination_table = job_config["tag_extract_destination_table"]
tmpl_table_schema = read_json(job_config["template_extract_table_schema"])
tag_table_schema = read_json(job_config["tag_extract_table_schema"])
bq_view_data_catalog_bigquery_info = job_config["bq_view_data_catalog_bigquery_info"]
bq_view_data_catalog_bi_tag_extract = job_config["bq_view_data_catalog_bi_tag_extract"]

function_data_catalogs = job_config["function_data_catalogs"]
function_data_extract = job_config["function_data_extract"]
trigger_bucket = job_config["trigger_bucket"]
service_account = job_config["service_account"]

######### ################## ######### ######### #########
######### ############ Creating GCS Buckets and Folders
######### ################## ######### ######### #########
gcs_client = storage.Client(project=project_id)

bucket = gcs_client.bucket(template_landing_bucket)
bucket.storage_class = "STANDARD"
bucket.iam_configuration.uniform_bucket_level_access_enabled = True
bucket = gcs_client.create_bucket(bucket, location=resource_location)
blob = bucket.blob('tags/')
blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
blob = bucket.blob('taxonomies/')
blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
blob = bucket.blob('templates/')
blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
print ('Bucket creation completed for tags and template landing area')

bucket = gcs_client.bucket(template_archive_bucket)
bucket.storage_class = "STANDARD"
bucket.iam_configuration.uniform_bucket_level_access_enabled = True
bucket = gcs_client.create_bucket(bucket, location=resource_location)
blob = bucket.blob('tags/error/')
blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
blob = bucket.blob('taxonomies/')
blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
blob = bucket.blob('templates/')
blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
print ('Bucket creation completed for archival area')

bucket = gcs_client.bucket(extract_bucket)
bucket.storage_class = "STANDARD"
bucket.iam_configuration.uniform_bucket_level_access_enabled = True
bucket = gcs_client.create_bucket(bucket, location=resource_location)
blob = bucket.blob('catalog_extract/')
blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
print ('Bucket creation completed for data extract area for reporting')


######### ################## ######### ######### #########
######### ############ Create BQ datasets and tables for reporting
######### ################## ######### ######### #########
client = bigquery.Client(project=project_id)

client.delete_dataset(f"{project_id}.{reporting_dataset}", delete_contents=True, not_found_ok=True)  # Make an API request.
print("Deleted dataset '{}'.".format(f"{project_id}.{reporting_dataset}"))

dataset = bigquery.Dataset(f"{project_id}.{reporting_dataset}")
dataset.location = resource_location
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

table_schema = []
for key, value in tmpl_table_schema.items():
    table_schema.append(bigquery.SchemaField(key, value))
table = bigquery.Table(f"{project_id}.{reporting_dataset}.{tmpl_destination_table}", schema=table_schema)
table = client.create_table(table)  # Make an API request.
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

table_schema = []
for key, value in tag_table_schema.items():
    table_schema.append(bigquery.SchemaField(key, value))
table = bigquery.Table(f"{project_id}.{reporting_dataset}.{tag_destination_table}", schema=table_schema)
table = client.create_table(table)  # Make an API request.
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

### Create BQ views for reporting
sql = """
CREATE VIEW `{}.{}.{}`
AS select * except (datasetJoinkey) from
(SELECT catalog_name||schema_name datasetJoinkey,creation_time dataset_creation_time ,  last_modified_time dataset_last_modified_time 
FROM acn-uki-ds-data-ai-project.`region-europe-west2`.INFORMATION_SCHEMA.SCHEMATA) datasets
""".format(    project_id, reporting_dataset, bq_view_data_catalog_bigquery_info)
### print('Running view script : ', sql)
job = client.query(query=sql,location=resource_location)  # API request.
job.result()  # Waits for the query to finish.
print('Created new view "{}.{}.{}".'.format(job.destination.project,job.destination.dataset_id,job.destination.table_id ))


######### ################## ######### ######### #########
######### ############ Deploy Cloud Functions
######### ################## ######### ######### #########
print ('cloud function deployment is started. It may take upto 4 minutes')
try:
    shutil.rmtree('./temp/')
except FileNotFoundError:
    print("temp folder not available to delete")
    
os.mkdir('./temp/')
shutil.copy('./main.py', './temp')
shutil.copy('./requirements.txt', './temp')
shutil.copytree('./config', './temp/config' , dirs_exist_ok=True)
shutil.copytree('./utils', './temp/utils' , dirs_exist_ok=True)

## this is to deploy gcp function  for data catalog tagging
command_to_run_function_for_tagging = 'gcloud functions deploy ' + f"{function_data_catalogs}" +  \
' --entry-point create_template_and_tag ' \
' --runtime python37 ' \
' --region ' + f"{resource_location}" + \
' --trigger-resource ' + f"{trigger_bucket}" +  \
' --service-account ' + f"{service_account}" + \
' --trigger-event google.storage.object.finalize ' \
' --retry'
print('command_to_run_function for tagging : ' + command_to_run_function_for_tagging)
run_shell_cmd (command_to_run_function_for_tagging, './temp')

## this is to deploy gcp function for data extract
command_to_run_function_for_extract = 'gcloud functions deploy ' + f"{function_data_extract}"  +  \
' --entry-point extract_datacatalog_data ' \
' --runtime python37 ' \
' --region ' + f"{resource_location}" + \
' --trigger-http ' \
' --service-account ' + f"{service_account}" + \
' --allow-unauthenticated'
print('command_to_run_function for data extract : ' + command_to_run_function_for_extract)
run_shell_cmd (command_to_run_function_for_extract, './temp')

try:
    shutil.rmtree('./temp/')
except FileNotFoundError:
    print("Wrong file or file path")
print ('cloud function deployment completed')
