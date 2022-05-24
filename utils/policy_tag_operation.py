from google.cloud import bigquery
from google.cloud import datacatalog
from utils.utils import read_json, read_tag_csv
import utils.taxonomy_operation as taxo_opr
import os, csv
from utils.gcs_operation import list_file_gcs, download_file_gcs, move_file_gcs, upload_file_to_gcs


def create_policy_tag(display_name, description, taxonomy, parent_policy_tag = ""):
    client = datacatalog.PolicyTagManagerClient()
    policy_tag = datacatalog.PolicyTag()
    policy_tag.display_name = display_name
    policy_tag.description = description
    if parent_policy_tag != "":
        policy_tag.parent_policy_tag = parent_policy_tag
    policy_tag = client.create_policy_tag(parent = taxonomy, policy_tag = policy_tag)
    print(f"""Policy tag "{policy_tag.name}" created.""")
    return policy_tag

def list_policy_tags(taxonomy):
    client = datacatalog.PolicyTagManagerClient()
    request = datacatalog.ListPolicyTagsRequest()
    request.parent = taxonomy

    policy_tag = client.list_policy_tags(request=request)
    result = []
    for pt in policy_tag:
        result.append(pt)
    return result

def get_policy_tag(taxonomy, display_name):
    policy_tags = list_policy_tags(taxonomy)
    result = ""
    for pt in policy_tags:
        if pt.display_name == display_name:
            result = pt.name
    return result

def attach_policy_tag(project_id, dataset_name, table_name, column_list, policy_tag):
    client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.{dataset_name}.{table_name}"
    table = client.get_table(table_id)

    # get the original schema of the table
    original_schema = table.schema

    # update the original schema with policy tag
    new_schema = []
    attached_columns = []
    for field in original_schema:
        if field.name in column_list:
            new_schema.append(bigquery.SchemaField(name=field.name, 
                                                field_type=field.field_type,
                                                mode=field.mode, 
                                                description=field.description,
                                                fields=field.fields,
                                                policy_tags=bigquery.PolicyTagList(names=[policy_tag]),
                                                precision=field.precision,
                                                scale=field.scale,
                                                max_length=field.max_length))
            attached_columns.append(field.name)
        else:
            new_schema.append(bigquery.SchemaField(name=field.name, 
                                                field_type=field.field_type,
                                                mode=field.mode, 
                                                description=field.description,
                                                fields=field.fields,
                                                policy_tags=field.policy_tags,
                                                precision=field.precision,
                                                scale=field.scale,
                                                max_length=field.max_length))
        
    table.schema = new_schema
    table = client.update_table(table, ["schema"])
    print(f"Policy Tag added to {table_id} :")
    print(f"policy_tag = {policy_tag}")
    print(f"columns = {attached_columns}\n")
    return True

def read_and_attach_policy_tag():
    job_config = read_json("config/config.json")

    project_id = job_config["project_id"]
    landing_bucket = job_config["policy_tag_landing_bucket"]
    archive_bucket = job_config["policy_tag_archive_bucket"]
    policy_tag_folder = job_config["policy_tag_folder"]
    temp_folder = job_config["temp_folder"]

    err_rn = ""
    if job_config["run_local"]:

        for policy_tag_file in os.listdir("policy_tags/landing/"):
            if policy_tag_file.endswith(".csv"):
                policy_tag_info_list = read_tag_csv(f"policy_tags/landing/{policy_tag_file}")
                for policy_tag_info in policy_tag_info_list:

                    # call function to tag each row
                    taxonomy = taxo_opr.get_taxonomies(project_id, policy_tag_info["taxonomy_location"], policy_tag_info["taxonomy"])
                    policy_tag = get_policy_tag(taxonomy, policy_tag_info["policy_tag"])
                    result = attach_policy_tag(project_id, policy_tag_info["dataset_name"], policy_tag_info["table_name"],
                                                policy_tag_info["column_names"].split(';'), policy_tag)

                    # write error records to file
                    if result == False:
                        err_rn = policy_tag_file.replace("error_", "")
                        file_exist = os.path.exists(f"policy_tags/error/error_{err_rn}")
                        with open(f"tags/error/error_{err_rn}", 'a') as error_file:
                            writer = csv.DictWriter(error_file, policy_tag_info.keys())
                            if file_exist:
                                writer.writerow(policy_tag_info)
                            else:
                                writer.writeheader()
                                writer.writerow(policy_tag_info)
                    print("-"*50)
                
                os.rename(f"policy_tags/landing/{policy_tag_file}", f"policy_tags/processed/{policy_tag_file}.done")

    else:
        gcs_list = list_file_gcs(project_id, landing_bucket, f"{policy_tag_folder}/")
        for policy_tag_file in gcs_list:
            if policy_tag_file.endswith(".csv"):
                download_file_gcs(project_id, landing_bucket, policy_tag_file, f"{temp_folder}{policy_tag_file.split('/')[-1]}")
                policy_tag_info_list = read_tag_csv(f"{temp_folder}{policy_tag_file.split('/')[-1]}")
                for policy_tag_info in policy_tag_info_list:

                    # call function to tag each row
                    taxonomy = taxo_opr.get_taxonomies(project_id, policy_tag_info["taxonomy_location"], policy_tag_info["taxonomy"])
                    policy_tag = get_policy_tag(taxonomy, policy_tag_info["policy_tag"])
                    result = attach_policy_tag(project_id, policy_tag_info["dataset_name"], policy_tag_info["table_name"],
                                                policy_tag_info["column_names"].split(';'), policy_tag)

                    # write error records to file
                    if result == False:
                        err_rn = policy_tag_file.replace("error_", "").split('/')[-1]
                        file_exist = os.path.exists(f"{temp_folder}error/error_{err_rn}")
                        if not file_exist:
                            os.makedirs(f"{temp_folder}error/")

                        with open(f"{temp_folder}error/error_{err_rn}", 'a') as error_file:
                            writer = csv.DictWriter(error_file, policy_tag_info.keys())
                            if file_exist:
                                writer.writerow(policy_tag_info)
                            else:
                                writer.writeheader()
                                writer.writerow(policy_tag_info)
                    print("-"*50)
                
                # upload error file to gcs
                if os.path.exists(f"{temp_folder}error/error_{err_rn}"):    
                    upload_file_to_gcs(project_id, archive_bucket, f"{temp_folder}error/error_{err_rn}", f"tags/error/error_{err_rn}")
                    os.remove(f"{temp_folder}error/error_{err_rn}")
                    os.removedirs(f"{temp_folder}error/")
                    print("-"*50)
                
                os.remove(f"{temp_folder}{policy_tag_file.split('/')[-1]}")
                move_file_gcs(project_id, landing_bucket, policy_tag_file, archive_bucket, f"{policy_tag_folder}/{policy_tag_file.split('/')[-1]}.done")



def auto_attach_policy_tag(tag_info):

    job_config = read_json("config/config.json")
    project_id = job_config["project_id"]
    default_taxonomy = job_config["default_taxonomy"]
    default_taxonomy_loc = job_config["default_taxonomy_location"]
    taxonomy_info = {
        "taxonomy_display_name": default_taxonomy,
        "location": default_taxonomy_loc,
        "description": "Data Sensitivity ranking for business data",
        "policy_tags": [
            {
                "display_name": "PII Data",
                "description": "High sensitivity"
            },
            {
                "display_name": "Sensitive Data",
                "description": "Medium sensitivity"
            }
        ]
    }

    if "auto_policy_tag" in tag_info.keys() and tag_info["auto_policy_tag"]:
        print("\nAuto policy tag is enabled.")
        taxo_opr.create_taxonomy(project_id, taxonomy_info)     #create default taxonomy
        taxonomy = taxo_opr.get_taxonomies(project_id, default_taxonomy_loc, default_taxonomy)
        if ("dataset_name" in tag_info.keys() and tag_info["dataset_name"] != "" 
            and "table_name" in tag_info.keys() and tag_info["table_name"] != ""):

            if "pii_columns" in tag_info.keys() and tag_info["pii_columns"] != "":
                policy_tag = get_policy_tag(taxonomy, "PII Data")
                attach_policy_tag(project_id, tag_info["dataset_name"], tag_info["table_name"], tag_info["pii_columns"].split(';'), policy_tag)
        
            if "sensitive_columns" in tag_info.keys() and tag_info["sensitive_columns"] != "":
                policy_tag = get_policy_tag(taxonomy, "Sensitive Data")
                attach_policy_tag(project_id, tag_info["dataset_name"], tag_info["table_name"], tag_info["sensitive_columns"].split(';'), policy_tag)
            return True

        else:
            print("'dataset_name' and 'table_name' are required for policy tagging")
            return False
    return True