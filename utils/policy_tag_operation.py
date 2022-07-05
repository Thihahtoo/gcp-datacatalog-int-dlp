from google.cloud import bigquery
from google.cloud import datacatalog
from utils.utils import read_json, read_tag_csv
import utils.taxonomy_operation as taxo_opr
import os, csv
from utils.gcs_operation import list_file_gcs, download_file_gcs, move_file_gcs, upload_file_to_gcs
import utils.dlp_operation as dlp_opr

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
    dlp_taxonomy = job_config["dlp_taxonomy"]
    dlp_taxonomy_loc = job_config["dlp_taxonomy_location"]

    dlp_info_types = job_config["dlp_info_types"]
    min_count = job_config["min_count"]
    max_rows = job_config["max_rows"]
    dlp_location = job_config["dlp_location"]
    topic_id = job_config["topic_id"]
    sub_id = job_config["sub_id"]
    dlp_timeout = job_config["dlp_timeout"]

    policy_tags = []
    for info in dlp_info_types:
        policy_tags.append({
            "display_name": info,
            "description": f"DLP generated tag for infoType: {info}"
        })

    taxonomy_info = {
        "taxonomy_display_name": dlp_taxonomy,
        "location": dlp_taxonomy_loc,
        "description": "DLP generated taxonomy for auto policy tagging for business critical data",
        "policy_tags": policy_tags
    }

    if "auto_policy_tag" in tag_info.keys() and tag_info["auto_policy_tag"]:
        print("\nAuto policy tag is enabled.")

        taxo_opr.create_taxonomy(project_id, taxonomy_info)     #create default taxonomy
        taxonomy = taxo_opr.get_taxonomies(project_id, dlp_taxonomy_loc, dlp_taxonomy)

        # attach policy tag using dlp
        dlp_opr.create_bq_dlp_table(project_id, tag_info["dataset_name"], tag_info["table_name"]+"_DLP")
        dlp_table_name = dlp_opr.create_dlp_job(project_id, tag_info["dataset_name"], tag_info["table_name"], dlp_info_types,
                                                max_rows, dlp_location, topic_id, sub_id, dlp_timeout)
        dlp_fields = dlp_opr.read_dlp_from_bq_table(project_id, tag_info["dataset_name"], dlp_table_name, min_count)
        
        print("DLP Generated fields:")
        print(dlp_fields)

        for info_dict in dlp_fields:
            column_name = info_dict["field_name"]
            tag_name = info_dict["info_types"]
            policy_tag = get_policy_tag(taxonomy, tag_name)
            attach_policy_tag(project_id, tag_info["dataset_name"], tag_info["table_name"], column_list=[column_name], policy_tag=policy_tag)

        # clean up after running the job
        # dlp_opr.delete_dlp_bq_table(project_id, tag_info["dataset_name"], dlp_table_name)

    return True

