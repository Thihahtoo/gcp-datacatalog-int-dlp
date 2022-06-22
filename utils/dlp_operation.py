from google.cloud import dlp_v2, bigquery, pubsub_v1
from utils.policy_tag_operation import attach_policy_tag, get_policy_tag
from utils.gcs_operation import list_file_gcs, read_json_gcs, move_file_gcs
from utils.taxonomy_operation import create_taxonomy, get_taxonomies
from utils.utils import read_json
import threading

def create_dlp_job(project_id, dataset_id, table_id, info_types, row_limit, location, topic_id, sub_id, timeout):

    dlp_client = dlp_v2.DlpServiceClient()

    topic = pubsub_v1.PublisherClient.topic_path(project_id, topic_id)
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, sub_id)

    parent = f"projects/{project_id}/locations/{location}"

    inspect_job_data = {
        "storage_config": {
            "big_query_options": {
                "table_reference": {
                    "project_id": project_id,
                    "dataset_id": dataset_id,
                    "table_id": table_id
                },
                "rows_limit": row_limit,
                "sample_method": "RANDOM_START"
            }
        },
        "inspect_config": {
            "info_types": [{"name": info_type} for info_type in info_types],
            "exclude_info_types": False,
            "include_quote": False
        },
        "actions": [
            {
                "save_findings": {
                    "output_config": {
                        "table": {
                            "project_id": project_id,
                            "dataset_id": dataset_id,
                            "table_id": table_id + "_DLP"
                        }
                    }
                }
            },
            {"pub_sub": {"topic": topic}}
        ]
    }
    
    operation = dlp_client.create_dlp_job(
        parent=parent, inspect_job=inspect_job_data
    )

    job_done = threading.Event()

    def callback(message):
        try:
            if message.attributes["DlpJobName"] == operation.name:
                message.ack()
                print("DLP Job Completed")
                job_done.set()
            else:
                message.drop()
        except Exception as e:
            print(e)
            raise

    subscriber.subscribe(subscription_path, callback=callback)
    finished = job_done.wait(timeout=timeout)
    if not finished:
        print("Job timed out.")

    return table_id + "_DLP"

def read_dlp_from_bq_table(project_id, dataset_id, table_name, min_count):
    bq_client = bigquery.Client(project=project_id)

    query = f"""
    SELECT
        table_counts.field_name,
        STRING_AGG( table_counts.name
            ORDER BY
            table_counts.count_total DESC
        ) AS infoTypes
    FROM (
        SELECT
            locations.record_location.field_id.name AS field_name,
            info_type.name,
            COUNT(*) AS count_total
        FROM
            {project_id}.{dataset_id}.{table_name},
            UNNEST(location.content_locations) AS locations
        WHERE
            (likelihood = 'LIKELY'
            OR likelihood = 'VERY_LIKELY'
            OR likelihood = 'POSSIBLE')
        GROUP BY
            locations.record_location.field_id.name,
            info_type.name
        HAVING
            count_total>{str(min_count)} 
    ) AS table_counts
    GROUP BY
        table_counts.field_name
    ORDER BY
        table_counts.field_name
    """

    results = bq_client.query(query=query)
    rows = results.result()
    dlp_values = []
    for row in rows:
        info_types = "DLP-" + row.get('infoTypes')
        main_info_type = info_types.split(",",1)[0] if "," in info_types else info_types
        dlp_values.append({"field_name": row.get('field_name'), "info_types": main_info_type})
    print("Successfully extracted DLP results.")
    return dlp_values

def clean_up_dlp(project_id, dataset_id, table_id):
    delete_bq_table(project_id, dataset_id, table_id)

def delete_dlp_job(project_id, job_id):
    dlp_client = dlp_v2.DlpServiceClient()
    name = f"projects/{project_id}/dlpJobs/{job_id}"
    dlp_client.delete_dlp_job(request={"name": name})

def delete_bq_table(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    if(table_id[-4:]==("_DLP")):
        table_name = f"{project_id}.{dataset_id}.{table_id}"
    else:
        table_name = f"{project_id}.{dataset_id}.{table_id}_DLP"
    client.delete_table(table_name, not_found_ok=True)

def add_tags_from_dlp(project_id, dataset_id, table_id, field_and_info_dicts, taxonomy, location):
    taxonomy = get_taxonomies(project_id, location, taxonomy)
    for info_dict in field_and_info_dicts:
        column_name = info_dict["field_name"]
        tag_name = info_dict["info_types"]
        policy_tag = get_policy_tag(taxonomy, tag_name)
        attach_policy_tag(project_id, dataset_id, table_id, column_list=[column_name], policy_tag=policy_tag)
    return ""

def create_taxonomy_from_dlp(project_id, location, dlp_fields, taxonomy_name):
    policy_tags = generate_policy_tags(dlp_fields)
    taxonomy_info = {
        "taxonomy_display_name": taxonomy_name,
        "location": location,
        "description": "DLP generated taxonomy for business sensitivity",
        "policy_tags": policy_tags
    }
    create_taxonomy(project_id, taxonomy_info) 
    return ""

def generate_policy_tags(dlp_fields):
    tags = []
    for field in dlp_fields:
        tags.append({
            "display_name": field['info_types'],
            "description": f"DLP generated tag for infoType: {field['info_types'][4:]}"
        })
    return tags

def extract_dlp_config():
    job_config = read_json("config/config.json")
    project_id = job_config["project_id"]
    landing_bucket = job_config["dlp_landing_bucket"]
    archive_bucket = job_config["dlp_archive_bucket"]
    dlp_folder = job_config["dlp_folder"]
    file_list = list_file_gcs(project_id, landing_bucket, dlp_folder)
    for dlp_file in file_list:
        if(dlp_file[-5:] == (".json")):
            json_info = read_json_gcs(project_id, landing_bucket, dlp_file)
            [result, dataset_id, dlp_table_name] = run_dlp_from_config(json_info)

            if(result):
                move_file_gcs(project_id, landing_bucket, dlp_file, archive_bucket, f"{dlp_folder}/{dlp_file.split('/')[-1]}.done")
                # clean_up_dlp(project_id, dataset_id, dlp_table_name)

def run_dlp_from_config(config_json):
    try:
        project_id = config_json['project_id']
        dataset_id = config_json['dataset_id']
        table_name = config_json["table_name"]
        info_types = config_json["info_types"]
        min_count = config_json["min_count"]
        max_rows = config_json["max_rows"]
        location = config_json["location"]
        taxonomy_name = config_json["taxonomy_name"]
        taxonomy_location = config_json["taxonomy_location"]
        topic_id = config_json["topic_id"]
        sub_id = config_json["sub_id"]
        dlp_timeout = config_json["dlp_timeout"]

        # create_bq_dlp_table(project_id,dataset_id,table_name+"_DLP")
        dlp_table_name = create_dlp_job(project_id, dataset_id, table_name, info_types, max_rows, location, topic_id, sub_id, dlp_timeout)
        dlp_fields = read_dlp_from_bq_table(project_id, dataset_id, dlp_table_name, min_count)
        create_taxonomy_from_dlp(project_id, taxonomy_location, dlp_fields, taxonomy_name)
        add_tags_from_dlp(project_id, dataset_id, table_name, dlp_fields, taxonomy_name, taxonomy_location)
        return [True, dataset_id, dlp_table_name]
    except Exception as e:
        print(e)
        return [False, "", ""]

def create_bq_dlp_table(project_id, dataset_id, table_name):

    bq_client = bigquery.Client()
    schema_json = read_json("config/dlp_bq_table_schema.json")
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    formatted_schema = []

    for row in schema_json:
        print(row)
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    print(formatted_schema)

    table = bigquery.Table(table_id, schema=formatted_schema)
    print(table)
    table = bq_client.create_table(table)
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    return ""