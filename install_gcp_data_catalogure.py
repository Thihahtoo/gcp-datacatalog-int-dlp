from google.cloud import storage
from google.cloud import bigquery
from utils.utils import run_shell_cmd , read_json

## getting config values
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

### Creating GCS Buckets and Folders
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

### Create BQ datasets and tables for reporting
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

# sql = """
# CREATE VIEW `acn-uki-ds-data-ai-project.temp_data_catalog_extract.data_catalog_bigquery_info`
# AS select * except (datasetJoinkey, joinkey ) from
# (SELECT catalog_name||schema_name datasetJoinkey,creation_time dataset_creation_time ,  last_modified_time dataset_last_modified_time 
# FROM acn-uki-ds-data-ai-project.`region-europe-west2`.INFORMATION_SCHEMA.SCHEMATA) datasets
# inner join 
# (select project_id||dataset_id datasetJoinkey , project_id||dataset_id||table_id joinkey, project_id,dataset_id,        table_id, TIMESTAMP_MILLIS(creation_time) table_creation_time,  TIMESTAMP_MILLIS(last_modified_time) table_last_modified_time,row_count      table_row_count,        size_bytes table_size_bytes      
# from acn-uki-ds-data-ai-project.`region-europe-west2`.INFORMATION_SCHEMA.TABLES) tables on datasets.datasetJoinkey = tables.datasetJoinkey
# inner join 
# (SELECT table_catalog||table_schema||table_name joinkey, column_name,is_nullable column_is_nullable,data_type column_data_type   
# FROM acn-uki-ds-data-ai-project.`region-europe-west2`.INFORMATION_SCHEMA.COLUMNS
# ) columns on tables.joinkey = columns.joinkey
# """.format(    project_id, reporting_dataset, bq_view_data_catalog_bigquery_info, project_id, resource_location, project_id, reporting_dataset, project_id, reporting_dataset)
# print('Running view create script "{}" ', sql)
# job = client.query(query=sql,location=resource_location)  # API request.
# job.result()  # Waits for the query to finish.
# print('Created new view "{}.{}.{}".'.format(job.destination.project,job.destination.dataset_id,job.destination.table_id ))

# sql = """
# CREATE VIEW `{}.{}.{}`
# AS SELECT distinct bq_info.project_id,bq_info.dataset_id dataset_name,bq_info.table_id table_name,bq_info.column_name,tags.template_id,
# tags.template_location,tags.tag_field_id,tags.tag_field_value,tags.extract_timestamp FROM data_catalog_extract.data_catalog_bigquery_info bq_info
# left join data_catalog_extract.data_catalog_tag_extract tags on  bq_info.project_id =tags.project_id
# and tags.dataset_name = bq_info.dataset_id and tags.table_name = bq_info.table_id and tags.column_name = bq_info.column_name
# union all 
# SELECT project_id,	dataset_name,	table_name,column_name,template_id,template_location,tag_field_id,
# tag_field_value,extract_timestamp FROM data_catalog_extract.data_catalog_tag_extract tags
# where dataset_name in ('data_catalog_dev','data_catalog_extract')
# """.format(    project_id, reporting_dataset, bq_view_data_catalog_bi_tag_extract)
# print('Running view create script "{}" ', sql)
# job = client.query(sql)  # API request.
# job.result()  # Waits for the query to finish.
# print('Created new view "{}.{}.{}".'.format(job.destination.project,job.destination.dataset_id,job.destination.table_id ))

### Deploy Cloud Functions
print ('cloud function deployment is started. It may take upto 4 minutes')
run_shell_cmd ('sh -x ./deploy_cloudfunction.sh')
print ('cloud function deployment completed')
