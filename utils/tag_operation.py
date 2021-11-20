from utils.utils import run_shell_cmd, read_json, read_tag_csv, prepare_dict
from utils.gcs_operation import list_file_gcs, download_file_gcs, move_file_gcs
from utils.tmpl_operation import get_template, get_latest_template_id
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
        if tag.template == related_template and tag.column == "":
            already_exist = True
            tag_entry_name = tag.name

    # delete if the tag is already existed.
    if already_exist == True:
        print("Table tag with given template already existed.")
        request = datacatalog.DeleteTagRequest()
        request.name = tag_entry_name
        result = datacatalog_client.delete_tag(request=request)
        print("Tag Deleted.")

    return True

def remove_column_tag(table_entry, project, template, template_location, column_name):
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
        if tag.template == related_template and tag.column == column_name:
            already_exist = True
            tag_entry_name = tag.name

    # delete if the tag is already existed.
    if already_exist == True:
        print("Column tag with given template already existed.")
        request = datacatalog.DeleteTagRequest()
        request.name = tag_entry_name
        result = datacatalog_client.delete_tag(request=request)
        print("Column Tag Deleted.")

    return True

def attach_table_tag(project, dataset, table, template, template_location, tag_info):
    # create a tag for a table
    datacatalog_client = datacatalog.DataCatalogClient()
    tag = datacatalog.Tag()

    # get template definition to define field types
    print(f"Creating tag using template : {template}, location: {template_location}")
    tmpl = get_template(project, template, template_location)

    tag.template = tmpl.name

    # prepare dictionary for correct data types
    tag_info = prepare_dict(tag_info)
    
    # get fields from template to filter fields which are only availabe in template
    tmpl_field = [field for field in tmpl.fields]

    for key, value in tag_info.items():

        if key in tmpl_field:
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

    # check column level tagging or not
    if "column_name" in tag_info.keys():
        tag.column = tag_info["column_name"]
        # remove column tag if existed
        remove_column_tag(table_entry, project, template, template_location, tag_info["column_name"])
        tag = datacatalog_client.create_tag(parent=table_entry, tag=tag)
        print(f"Created Column Tag: {tag.name}")
    else:
        # remove table tag if existed
        remove_table_tag(table_entry, project, template, template_location)
        tag = datacatalog_client.create_tag(parent=table_entry, tag=tag)
        print(f"Created Table Tag: {tag.name}")

    return True


def read_and_attach_tag():
    job_config = read_json("config/config.json")

    project_id = job_config["project_id"]
    landing_bucket = job_config["tag_landing_bucket"]
    archive_bucket = job_config["tag_archive_bucket"]
    tag_folder = job_config["tag_folder"]
    temp_folder = job_config["temp_folder"]
    tmpl_prefix = job_config["template_prefix"]
    default_tmpl = job_config["default_template"]
    default_tmpl_loc = job_config["default_template_location"]
    col_tmpl_prefix = job_config["column_template_prefix"]
    default_col_tmpl = job_config["default_column_template"]
    default_col_tmpl_loc = job_config["default_column_template_location"]

    latest_tmpl = get_latest_template_id(project_id, tmpl_prefix, default_tmpl_loc)
    if latest_tmpl != "":
        default_tmpl = latest_tmpl

    # for column level tagging
    latest_col_tmpl = get_latest_template_id(project_id, col_tmpl_prefix, default_col_tmpl_loc)
    if latest_col_tmpl != "":
        default_col_tmpl = latest_col_tmpl

    if job_config["run_local"]:
        for tag_file in os.listdir("tags/landing/"):
            if tag_file.endswith(".csv"):
                tag_info_list = read_tag_csv(f"tags/landing/{tag_file}")
                for tag_info in tag_info_list:
                    dataset = tag_info['dataset_name']
                    table = tag_info['table_name']

                    # use default template if template id is not provided
                    if 'template_id' in tag_info.keys() and tag_info['template_id'] != "":
                        template = tag_info['template_id']
                    else:
                        # check column level tag or not
                        if "column_name" in tag_info.keys() and tag_info["column_name"] != "":
                            template = default_col_tmpl
                        else:
                            template = default_tmpl

                    # use default template location if template location is not provided
                    if 'template_location' in tag_info.keys() and tag_info['template_location'] != "":
                        tmplt_loc = tag_info['template_location']
                    else:
                        # check column level tag or not
                        if "column_name" in tag_info.keys() and tag_info["column_name"] != "":
                            tmplt_loc = default_col_tmpl_loc
                        else:
                            tmplt_loc = default_tmpl_loc

                    # attach tags
                    attach_table_tag(project_id, dataset, table, template, tmplt_loc, tag_info)
                    print("-"*50)
            
                os.rename(f"tags/landing/{tag_file}", f"tags/processed/{tag_file}.done")

    else:
        gcs_list = list_file_gcs(project_id, landing_bucket, f"{tag_folder}/")
        for tag_file in gcs_list:
            if tag_file.endswith(".csv"):
                download_file_gcs(project_id, landing_bucket, tag_file, f"{temp_folder}{tag_file.split('/')[-1]}")
                tag_info_list = read_tag_csv(f"{temp_folder}{tag_file.split('/')[-1]}")
                for tag_info in tag_info_list:
                    dataset = tag_info['dataset_name']
                    table = tag_info['table_name']

                    # use default template if template id is not provided
                    if 'template_id' in tag_info.keys() and tag_info['template_id'] != "":
                        template = tag_info['template_id']
                    else:
                        # check column level tag or not
                        if "column_name" in tag_info.keys() and tag_info["column_name"] != "":
                            template = default_col_tmpl
                        else:
                            template = default_tmpl

                    # use default template location if template location is not provided
                    if 'template_location' in tag_info.keys() and tag_info['template_location'] != "":
                        tmplt_loc = tag_info['template_location']
                    else:
                        # check column level tag or not
                        if "column_name" in tag_info.keys() and tag_info["column_name"] != "":
                            tmplt_loc = default_col_tmpl_loc
                        else:
                            tmplt_loc = default_tmpl_loc

                    # attach tags
                    attach_table_tag(project_id, dataset, table, template, tmplt_loc, tag_info)
                    print("-"*50)
                    
                os.remove(f"{temp_folder}{tag_file.split('/')[-1]}")
                move_file_gcs(project_id, landing_bucket, tag_file, archive_bucket, f"{tag_folder}/{tag_file.split('/')[-1]}.done")

    return True