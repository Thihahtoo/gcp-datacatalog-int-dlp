from google.cloud import storage
from google.cloud import bigquery
from utils.utils import run_shell_cmd , read_json
from google.cloud.exceptions import NotFound
import os
import shutil

######### ################## ################## #########
######### ########### getting config values
######### ################## ################## #########
print ('****************** ****************** ****************** ******************')
print ('****************** Preparing for deployment ******************')
print ('****************** ****************** ****************** ******************')
condition = input ('Make sure: you have configured the GCP project id in config/config.json and also configured the credentials to call GCP APIs [check https://cloud.google.com/docs/authentication]. Do you want to continue ? Type y for yes')
if condition != 'y':
    print ('Exiting the deployment')
    exit()
else:
    print ('Starting the deployment')
    print ('Reading the config/config.json file for configuration values')

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
#########  doing validation against GCS , BQ and CloudFunction ############
######### ################## ######### ######### #########
gcs_client = storage.Client(project=project_id)
client = bigquery.Client(project=project_id)

#########  checking GCS buckets ############
bucket = [template_landing_bucket, template_archive_bucket, extract_bucket]
for i in bucket:
    BUCKET = gcs_client.bucket(i)
    if BUCKET.exists():
        BUCKET = gcs_client.get_bucket(i)
        exit('ERROR : GCS Bucket is already available : ' + i + ' : Delete this bucket first and then re run')

#########  Creating BQ datasets ############
client.delete_dataset(f"{project_id}.{reporting_dataset}", delete_contents=True, not_found_ok=True)  # Make an API request.
print("Deleted BQ dataset '{}'.".format(f"{project_id}.{reporting_dataset}"))

try:
    client.get_dataset(f"{project_id}.{reporting_dataset}")  # Make an API request.
    exit("ERROR : BQ Dataset {} already exists".format(f"{project_id}.{reporting_dataset}"))
except NotFound:
    None

######### ################## ######### ######### #########
#########  Creating GCS Buckets and Folders ############
######### ################## ######### ######### #########
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
print ('GCS Bucket creation completed for tags and template landing area')

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
print ('GCS Bucket creation completed for archival area')

bucket = gcs_client.bucket(extract_bucket)
bucket.storage_class = "STANDARD"
bucket.iam_configuration.uniform_bucket_level_access_enabled = True
bucket = gcs_client.create_bucket(bucket, location=resource_location)
blob = bucket.blob('catalog_extract/')
blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
print ('GCS Bucket creation completed for data extract area for reporting')

######### ################## ######### ######### #########
######### ############ Create BQ datasets and tables for reporting
######### ################## ######### ######### #########
client.delete_dataset(f"{project_id}.{reporting_dataset}", delete_contents=True, not_found_ok=True)  # Make an API request.
print("Deleted BQ dataset '{}'.".format(f"{project_id}.{reporting_dataset}"))

dataset = bigquery.Dataset(f"{project_id}.{reporting_dataset}")
dataset.location = resource_location
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created new BQ dataset {}.{}".format(client.project, dataset.dataset_id))

table_schema = []
for key, value in tmpl_table_schema.items():
    table_schema.append(bigquery.SchemaField(key, value))
table = bigquery.Table(f"{project_id}.{reporting_dataset}.{tmpl_destination_table}", schema=table_schema)
table = client.create_table(table)  # Make an API request.
print("Created new BQ table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

table_schema = []
for key, value in tag_table_schema.items():
    table_schema.append(bigquery.SchemaField(key, value))
table = bigquery.Table(f"{project_id}.{reporting_dataset}.{tag_destination_table}", schema=table_schema)
table = client.create_table(table)  # Make an API request.
print("Created BQ table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

### Create BQ views for reporting
sql_bq_procedure = """CREATE OR REPLACE PROCEDURE `{}.{}.{}`(in_project_id STRING, in_resource_location STRING, in_extract_destination_dataset STRING, in_bq_view_data_catalog_bigquery_info STRING)
BEGIN
  DECLARE  db_view_sql_statement , sql_dataset STRING(4000);
  DECLARE  counter INT64;
  SET db_view_sql_statement = concat ('create or replace view  ', in_project_id , '.',in_extract_destination_dataset ,'.' , in_bq_view_data_catalog_bigquery_info , ' as ( select * except (datasetJoinkey, joinkey ) from ( ');
  SET sql_dataset =  concat ('SELECT schema_name dataset_id, catalog_name||schema_name datasetJoinkey,creation_time dataset_creation_time ,	last_modified_time dataset_last_modified_time FROM ' , in_project_id , '.', '`region-',in_resource_location , '`.INFORMATION_SCHEMA.SCHEMATA ');
  
  EXECUTE IMMEDIATE concat('CREATE TEMP TABLE tmp_dateset AS ',sql_dataset);

  SET db_view_sql_statement = concat (db_view_sql_statement , sql_dataset);
  SET db_view_sql_statement = concat (db_view_sql_statement , ' ) datasets inner join ( ');
  
  
  SET counter = 0;
  FOR list in ( select dataset_id from tmp_dateset )
  DO
    if counter != 0 then 
      SET db_view_sql_statement = concat (db_view_sql_statement ,' union all ');
    end if;	
    SET db_view_sql_statement = concat (db_view_sql_statement , ' select project_id||dataset_id datasetJoinkey , project_id||dataset_id||table_id joinkey, project_id,	table_id, TIMESTAMP_MILLIS(creation_time) table_creation_time,	TIMESTAMP_MILLIS(last_modified_time) table_last_modified_time,row_count	table_row_count,	size_bytes table_size_bytes	 from ' , in_project_id , '.', list.dataset_id ,'.__TABLES__ ');
    SET counter = 1;
  END FOR;  
  SET db_view_sql_statement = concat (db_view_sql_statement , ' ) tables on datasets.datasetJoinkey = tables.datasetJoinkey inner join ( ');
  
  
  SET counter = 0;
  FOR list in (select dataset_id from tmp_dateset)
  DO
    if counter != 0 then 
      SET db_view_sql_statement = concat (db_view_sql_statement ,' union all ');
    end if;	
    SET db_view_sql_statement = concat (db_view_sql_statement , ' SELECT table_catalog||table_schema||table_name joinkey, column_name,is_nullable column_is_nullable,data_type column_data_type	 FROM ', in_project_id , '.', list.dataset_id ,'.INFORMATION_SCHEMA.COLUMNS ');
    SET counter = 1;
  END FOR;
  SET db_view_sql_statement = concat (db_view_sql_statement , ' ) columns on tables.joinkey = columns.joinkey )');
  
  EXECUTE IMMEDIATE db_view_sql_statement;  

  SET db_view_sql_statement = concat ( 'create or replace view `{}.{}.{}` as   SELECT distinct bq_info.project_id,bq_info.dataset_id dataset_name,bq_info.table_id table_name,bq_info.column_name, ');
  SET db_view_sql_statement = concat (db_view_sql_statement , ' tags.template_id,tags.template_location,tags.tag_field_id,tags.tag_field_value,tags.extract_timestamp FROM {}.{} bq_info ');
  SET db_view_sql_statement = concat (db_view_sql_statement , ' left join {}.{} tags on  bq_info.project_id =tags.project_id and tags.dataset_name = bq_info.dataset_id and tags.table_name = bq_info.table_id and tags.column_name = bq_info.column_name ');
  SET db_view_sql_statement = concat (db_view_sql_statement , ' union all SELECT project_id,	dataset_name,	table_name, column_name, template_id, template_location, tag_field_id,  tag_field_value, extract_timestamp FROM {}.{} tags ');
  
  EXECUTE IMMEDIATE db_view_sql_statement;  

END; """.format(    project_id, reporting_dataset, bq_view_data_catalog_bigquery_info,
                    project_id, reporting_dataset,bq_view_data_catalog_bi_tag_extract, 
                    reporting_dataset, bq_view_data_catalog_bigquery_info , 
                    reporting_dataset,tag_destination_table,
                    reporting_dataset,tag_destination_table)
job = client.query(query=sql_bq_procedure,location=resource_location)  # API request.
job.result()  # Waits for the query to finish.
print('Created new BQ procedure "{}".'.format(bq_view_data_catalog_bigquery_info))

call_sql_bq_procedure = """CALL `{}.{}.{}` ('{}', '{}','{}' ,'{}');
""".format(    project_id, reporting_dataset, bq_view_data_catalog_bigquery_info, project_id, resource_location , reporting_dataset, bq_view_data_catalog_bigquery_info)
job = client.query(query=call_sql_bq_procedure,location=resource_location)  # API request.
job.result()  # Waits for the query to finish.
print('Completed BQ procedure run "{}".'.format(bq_view_data_catalog_bigquery_info))

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
