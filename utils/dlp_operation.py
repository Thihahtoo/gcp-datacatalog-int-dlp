from google.cloud import dlp_v2, bigquery
from utils.policy_tag_operation import attach_policy_tag, create_policy_tag
from utils.gcs_operation import list_file_gcs, read_json_gcs, move_file_gcs
from utils.utils import read_json

def create_dlp_job(project_id, dataset_id, table_id, info_types, row_limit, location):

    dlp_client = dlp_v2.DlpServiceClient()

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
            }
        ]
    }
    
    dlp_client.create_dlp_job(
        parent=parent, inspect_job=inspect_job_data
    )

    return ""

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
    return dlp_values

def clean_up_dlp(project_id, dataset_id, table_id):
    delete_bq_table(project_id, dataset_id, table_id)

    return ""

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

def add_tags_from_dlp(project_id, dataset_id, table_id, field_and_info_dicts, location):
    taxonomy = f"projects/{project_id}/locations/{location}"
    for info_dict in field_and_info_dicts:
        create_policy_tag(info_dict["info_types"], "DLP generated tag for {}".format(info_dict["field_name"]), taxonomy)
        attach_policy_tag(project_id,dataset_id, table_id, column_list=[info_dict["field_name"]], policy_tag=(info_dict["info_types"]))

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
            result = run_dlp_from_config(json_info)

            if(result):
                move_file_gcs(project_id, landing_bucket, dlp_file, archive_bucket, f"{dlp_folder}/{dlp_file.split('/')[-1]}.done")

def run_dlp_from_config(config_json):
    try:
        project_id = config_json['project_id']
        dataset_id = config_json['dataset_id']
        table_name = config_json["table_name"]
        info_types = config_json["info_types"]
        min_count = config_json["min_count"]
        max_rows = config_json["max_rows"]
        location = config_json["location"]

        create_dlp_job(project_id, dataset_id, table_name, info_types, max_rows, location)
        dlp_fields = read_dlp_from_bq_table(project_id, dataset_id, table_name + "_DLP", min_count)
        print(dlp_fields)
        # add_tags_from_dlp(project_id, dataset_id, table_name, dlp_fields, location)

        return True
    except:
        print("Something went wrong")
        return False