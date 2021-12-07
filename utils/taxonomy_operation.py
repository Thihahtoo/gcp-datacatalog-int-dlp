from google.cloud import datacatalog
from utils.utils import read_json
import os
from utils.gcs_operation import list_file_gcs, read_json_gcs, move_file_gcs
import utils.policy_tag_operation as pt

def list_taxonomies(project_id, location):
    client = datacatalog.PolicyTagManagerClient()
    request = datacatalog.ListTaxonomiesRequest()
    request.parent = f"projects/{project_id}/locations/{location}"

    taxonomy = client.list_taxonomies(request=request)
    result = []
    for t in taxonomy:
        result.append(t)
    return result

def get_taxonomies(project_id, location, display_name):
    taxonomies = list_taxonomies(project_id, location)
    result = ""
    for taxo in taxonomies:
        if taxo.display_name == display_name:
            result = taxo.name
    return result

def create_taxonomy(project_id, taxonomy_info):
    client = datacatalog.PolicyTagManagerClient()

    location = taxonomy_info["location"]
    display_name = taxonomy_info["taxonomy_display_name"]

    # create taxonomy
    taxonomy = datacatalog.Taxonomy()
    taxonomy.display_name = display_name
    if "description" in taxonomy_info.keys():
        taxonomy.description = taxonomy_info["description"]
    
    try:
        taxonomy = client.create_taxonomy(parent = f"projects/{project_id}/locations/{location}", taxonomy = taxonomy)
        print(f"""Taxonomy "{taxonomy.name}" created.""")

        # recursive function for sub_tag creation
        def sub_tag_creation(p_tag_info, parent_tag):
            if "sub_tag" in p_tag_info.keys():
                for tag in p_tag_info["sub_tag"]:
                    display_name = tag["display_name"]
                    if "description" in tag.keys():
                        description = tag["description"]
                    policy_tag = pt.create_policy_tag(display_name, description, taxonomy.name, parent_tag.name)
                    sub_tag_creation(tag, policy_tag)

        # create policy tag under taxonomy
        for tag in taxonomy_info["policy_tags"]:
            display_name = tag["display_name"]
            if "description" in tag.keys():
                description = tag["description"]
            policy_tag = pt.create_policy_tag(display_name, description, taxonomy.name)

            # replace the below code with recursive function
            sub_tag_creation(tag, policy_tag)

            # # sub_tag level 1
            # if "sub_tag" in tag.keys():
            #     for tag1 in tag["sub_tag"]:
            #         display_name = tag1["display_name"]
            #         if "description" in tag1.keys():
            #             description = tag1["description"]
            #         policy_tag1 = create_policy_tag(display_name, description, taxonomy.name, policy_tag.name)

            #         # sub_tag level 2
            #         if "sub_tag" in tag1.keys():
            #             for tag2 in tag1["sub_tag"]:
            #                 display_name = tag2["display_name"]
            #                 if "description" in tag2.keys():
            #                     description = tag2["description"]
            #                 policy_tag2 = create_policy_tag(display_name, description, taxonomy.name, policy_tag1.name)

            #                 # sub_tag level 3
            #                 if "sub_tag" in tag2.keys():
            #                     for tag3 in tag2["sub_tag"]:
            #                         display_name = tag3["display_name"]
            #                         if "description" in tag3.keys():
            #                             description = tag3["description"]
            #                         policy_tag3 = create_policy_tag(display_name, description, taxonomy.name, policy_tag2.name)

            #                         # sub_tag level 4
            #                         if "sub_tag" in tag3.keys():
            #                             for tag4 in tag3["sub_tag"]:
            #                                 display_name = tag4["display_name"]
            #                                 if "description" in tag4.keys():
            #                                     description = tag4["description"]
            #                                 policy_tag4 = create_policy_tag(display_name, description, taxonomy.name, policy_tag3.name)
        return True

    except Exception:
        print(f"""Taxonomy "{display_name}" already existed in "{location}".""")
        return False

def create_taxonomy_from_file():
    job_config = read_json("config/config.json")
    project_id = job_config["project_id"]
    landing_bucket = job_config["taxonomy_landing_bucket"]
    archive_bucket = job_config["taxonomy_archive_bucket"]
    taxonomy_folder = job_config["taxonomy_folder"]

    if job_config["run_local"]:
        for taxo_file in os.listdir("taxonomy/landing/"):
            if taxo_file.startswith("taxonomy") and taxo_file.endswith(".json"):
                taxonomy_info = read_json(f"taxonomy/landing/{taxo_file}")
                result = create_taxonomy(project_id, taxonomy_info)
                if result:
                    os.rename(f"taxonomy/landing/{taxo_file}", f"taxonomy/processed/{taxo_file}.done")
    else:
        gcs_list = list_file_gcs(project_id, landing_bucket, f"{taxonomy_folder}/taxonomy")
        for taxo_file in gcs_list:
            if taxo_file.endswith(".json"):
                taxonomy_info = read_json_gcs(project_id, landing_bucket, taxo_file)
                result = create_taxonomy(project_id, taxonomy_info)
                if result:
                    move_file_gcs(project_id, landing_bucket, taxo_file, archive_bucket, f"{taxonomy_folder}/{taxo_file.split('/')[-1]}.done")