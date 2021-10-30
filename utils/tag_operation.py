from utils.utils import run_shell_cmd, read_json, read_tag_csv, prepare_dict
from utils.gcs_operation import list_file_gcs, download_file_gcs, move_file_gcs
from utils.tmpl_operation import get_template
import os
from google.cloud import datacatalog

def get_table_entry(project, dataset, table):
    # retrieve a project.dataset.table entry
    datacatalog_client = datacatalog.DataCatalogClient()
    resource_name = (
        f"//bigquery.googleapis.com/projects/{project}"
        f"/datasets/{dataset}/tables/{table}"
    )
    print(f'table: {project}.{dataset}.{table}')
    table_entry = datacatalog_client.lookup_entry(request={"linked_resource": resource_name})
    return table_entry.name

def remove_table_tag(table_entry, project, template, template_location):
    # remove tag for a table

    # list the tag related with table
    datacatalog_client = datacatalog.DataCatalogClient()
    request = datacatalog.ListTagsRequest()
    request.parent = table_entry
    gdc_tag_result = datacatalog_client.list_tags(request=request)
    related_template = f"projects/{project}/locations/{template_location}/tagTemplates/{template}"
    already_exist = False
    tag_entry_name = ""
    for tag in gdc_tag_result.tags:
        if tag.template == related_template:
            already_exist = True
            tag_entry_name = tag.name

    # delete if the tag is already existed.
    if already_exist == True:
        print("Tag with given template already existed.")
        request = datacatalog.DeleteTagRequest()
        request.name = tag_entry_name
        result = datacatalog_client.delete_tag(request=request)
        print("Tag Deleted.")

    return True

def attach_table_tag(project, dataset, table, template, template_location, tag_json):
    # create a tag for a table
    datacatalog_client = datacatalog.DataCatalogClient()
    tag = datacatalog.Tag()

    # get template definition to define field types
    tmpl = get_template(project, template, template_location)

    tag.template = tmpl.name
    
    for key, value in tag_json.items():
        tag.fields[key] = datacatalog.TagField()

        # get the field type from template according to field
        field_type = tmpl.fields[key].type_

        if field_type.primitive_type:
            if str(field_type.primitive_type) == 'PrimitiveType.STRING':
                tag.fields[key].string_value = value
            if str(field_type.primitive_type) == 'PrimitiveType.DOUBLE':
                tag.fields[key].double_value = value
            if str(field_type.primitive_type) == 'PrimitiveType.BOOL':
                tag.fields[key].bool_value = value

        if field_type.enum_type:
            tag.fields[key].enum_value.display_name = value

    # get table entry and remove tag if existed.
    table_entry = get_table_entry(project, dataset, table)
    remove_table_tag(table_entry, project, template, template_location)

    tag = datacatalog_client.create_tag(parent=table_entry, tag=tag)
    print(f"Created tag: {tag.name}")

    return True

def read_and_attach_tag():
    job_config = read_json("config/config.json")

    project_id = job_config["project_id"]
    bucket = job_config["bucket"]
    landing = job_config["tag_landing_folder"]
    processed = job_config["tag_processed_folder"]
    temp_folder = job_config["temp_folder"]

    if job_config["run_local"]:
        for tag_file in os.listdir("tags/landing/"):
            if tag_file.endswith(".csv"):
                tag_info_list = read_tag_csv(f"tags/landing/{tag_file}")
                for tag_info in tag_info_list:
                    dataset = tag_info['dataset_name']
                    table = tag_info['table_name']
                    template = tag_info['template_id']
                    tmplt_loc = tag_info['template_location']
                    tag_json = prepare_dict(tag_info['tag_json'])
                    # dict_to_json(tag_json, 'temp_tag_info.json')
                    attach_table_tag(project_id, dataset, table, template, tmplt_loc, tag_json)
            
                os.rename(f"tags/landing/{tag_file}", f"tags/processed/{tag_file}.done")

    else:
        gcs_list = list_file_gcs(project_id, bucket, f"{landing}/")
        for tag_file in gcs_list:
            if tag_file.endswith(".csv"):
                download_file_gcs(project_id, bucket, tag_file, f"{temp_folder}{tag_file.split('/')[-1]}")
                tag_info_list = read_tag_csv(f"{temp_folder}{tag_file.split('/')[-1]}")
                for tag_info in tag_info_list:
                    dataset = tag_info['dataset_name']
                    table = tag_info['table_name']
                    template = tag_info['template_id']
                    tmplt_loc = tag_info['template_location']
                    tag_json = prepare_dict(tag_info['tag_json'])
                    # dict_to_json(tag_json, 'temp_tag_info.json')
                    attach_table_tag(project_id, dataset, table, template, tmplt_loc, tag_json)
                    
                os.remove(f"{temp_folder}{tag_file.split('/')[-1]}")
                move_file_gcs(project_id, bucket, tag_file, bucket, f"{processed}/{tag_file.split('/')[-1]}.done")

    return True