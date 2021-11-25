from google.cloud import bigquery
from google.cloud import datacatalog

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
    print(f"columns = {attached_columns}")
    print(f"policy_tag = {policy_tag}")
    print("-"*50)


# attach_policy_tag("acn-uki-ds-data-ai-project", "data_catalog_dev", "covid_worldwide", ["year", "deaths"], 905746461448123128, "eu", 6661519892503741752)

# print(list_policy_tags("projects/acn-uki-ds-data-ai-project/locations/eu/taxonomies/4939288784302392788"))