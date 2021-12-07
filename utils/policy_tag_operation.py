from google.cloud import bigquery
from google.cloud import datacatalog
from utils.utils import read_json
import utils.taxonomy_operation as taxo_opr

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