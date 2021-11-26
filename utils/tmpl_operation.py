from utils.utils import run_shell_cmd, read_json
from utils.gcs_operation import list_file_gcs, read_json_gcs, move_file_gcs
import os
from google.cloud import datacatalog

def create_template(project_id, template_id, location, display_name, fields):
    # Create a Tag Template.
    datacatalog_client = datacatalog.DataCatalogClient()
    tag_template = datacatalog.TagTemplate()

    tag_template.display_name = display_name

    for field in fields:
        tag_template.fields[field["id"]] = datacatalog.TagTemplateField()
        tag_template.fields[field["id"]].display_name = field["display_name"]
        tag_template.fields[field["id"]].is_required = field["required"]
        if "description" in field:
            tag_template.fields[field["id"]].description = field["description"]

        if field["type"] == "string":
            tag_template.fields[field["id"]].type_.primitive_type = datacatalog.FieldType.PrimitiveType.STRING
        if field["type"] == "double":
            tag_template.fields[field["id"]].type_.primitive_type = datacatalog.FieldType.PrimitiveType.DOUBLE
        if field["type"] == "bool":
            tag_template.fields[field["id"]].type_.primitive_type = datacatalog.FieldType.PrimitiveType.BOOL
        if field["type"] == "enum":
            for display_name in field["allowed_values"]:
                enum_value = datacatalog.FieldType.EnumType.EnumValue(display_name=display_name)
                tag_template.fields[field["id"]].type_.enum_type.allowed_values.append(enum_value)

    expected_template_name = datacatalog.DataCatalogClient.tag_template_path(
        project_id, location, template_id
    )
    # Create the Tag Template.
    try:
        tag_template = datacatalog_client.create_tag_template(
            parent=f"projects/{project_id}/locations/{location}",
            tag_template_id=template_id,
            tag_template=tag_template,
        )
        print(f"Created template: {tag_template.name}")
    except OSError as e:
        print(f"Cannot create template: {expected_template_name}")
        print(f"{e}")
    return True

def list_template(project_id):
    # list all the templates in project
    datacatalog_client = datacatalog.DataCatalogClient()
    scope = datacatalog.SearchCatalogRequest.Scope()
    scope.include_project_ids.append(project_id)
    result = []
    templates = datacatalog_client.search_catalog(scope=scope, query=f'type=tag_template')
    for tmpl in templates:
        result.append(tmpl.relative_resource_name)
    return result

def get_template_info(project_id, template_id, location):
    # get all the template info
    tmpl = get_template(project_id, template_id, location)
    
    result = []
    tmpl_field = str(tmpl).split('fields ')[1:]
    for field in tmpl_field:
        tmpl_info = {"project_id":"", "template_id":"", "template_loc":"", 
                    "field_id":"", "field_display_name":"", "field_type":"", "field_allowed_values":"",
                    "requried_field":False, "field_description":""}
        tmpl_info["project_id"] = project_id
        tmpl_info["template_id"] = template_id
        tmpl_info["template_loc"] = location
        tmpl_info["field_id"] = field.split('\n')[1].split(':')[1].strip().replace('"','')
        tmpl_info["field_display_name"] = (field.split('display_name:')[1].split('\n')[0].strip().replace('"','') 
                                            if "display_name" in field else "")

        if "enum_type" in field:
            tmpl_info["field_type"] = "ENUM"
            for value in field.split('type_')[1].split('\n'):
                if "display_name" in value:
                    tmpl_info["field_allowed_values"] = tmpl_info["field_allowed_values"] + value.split(':')[1].strip().replace('"','') + ";"
        else:
            tmpl_info["field_type"] = field.split('type_')[1].split('}')[0].split(':')[1].strip()
        
        tmpl_info["field_description"] = (field.split('description:')[1].split('\n')[0].strip().replace('"','') 
                                            if "description" in field else "")

        if "is_required" in field:
            tmpl_info["requried_field"] = True
        result.append(tmpl_info)
    return result

def check_template_exist(project_id, template_id, location):
    # check the tag template if it is existing
    templates = list_template(project_id)
    full_name = f"projects/{project_id}/locations/{location}/tagTemplates/{template_id}"
    if full_name in templates:
        print(f"Exist: {full_name}")
        return True
    else:
        return False

def get_template(project_id, template_id, location):
    # get template definition
    datacatalog_client = datacatalog.DataCatalogClient()
    request = datacatalog.GetTagTemplateRequest()
    request.name = f'projects/{project_id}/locations/{location}/tagTemplates/{template_id}'
    result = datacatalog_client.get_tag_template(request=request)
    return result

def delete_template(project_id, template_id, location):
    # check if existed and delete template
    datacatalog_client = datacatalog.DataCatalogClient()
    request = datacatalog.DeleteTagTemplateRequest()
    request.name = f'projects/{project_id}/locations/{location}/tagTemplates/{template_id}'
    request.force = True
    tmpl_exist = check_template_exist(project_id, template_id, location)
    if tmpl_exist:
        result = datacatalog_client.delete_tag_template(request=request)
        print(f"Deleted: {request.name}")
        return result

def get_latest_template_id(project_id, template_prefix, location):
    datacatalog_client = datacatalog.DataCatalogClient()
    scope = datacatalog.SearchCatalogRequest.Scope()
    scope.include_project_ids.append(project_id)
    results = datacatalog_client.search_catalog(scope=scope, query=f'type=tag_template location={location} name:{template_prefix}')
    fetched_results = [result.relative_resource_name for result in results]
    latest_tmpl = ""
    if fetched_results:
        if len(fetched_results) > 1:
            max_version = max([tmpl.split("_")[-1] for tmpl in fetched_results])
            for tmpl in fetched_results:
                if tmpl.split("_")[-1] == max_version:
                    latest_tmpl = tmpl.split("/")[-1]
        else:
            latest_tmpl = fetched_results[0].split("/")[-1]
        return latest_tmpl
    else:
        return ""

def create_tag_template_from_file():
    job_config = read_json("config/config.json")

    project_id = job_config["project_id"]
    landing_bucket = job_config["template_landing_bucket"]
    archive_bucket = job_config["template_archive_bucket"]
    template_folder = job_config["template_folder"]

    if job_config["run_local"]:
        for tmpl_file in os.listdir("tag_template/landing/"):
            if tmpl_file.startswith("template") and tmpl_file.endswith(".json"):
                tmpl_cfg = read_json(f"tag_template/landing/{tmpl_file}")
                # delete template when existed
                delete_template(project_id, tmpl_cfg["template_id"], tmpl_cfg["location"])
                result = create_template(project_id, tmpl_cfg["template_id"], tmpl_cfg["location"], tmpl_cfg["display_name"], tmpl_cfg["fields"])
                if result:
                    os.rename(f"tag_template/landing/{tmpl_file}", f"tag_template/processed/{tmpl_file}.done")
    else:
        gcs_list = list_file_gcs(project_id, landing_bucket, f"{template_folder}/template")
        for tmpl_file in gcs_list:
            if tmpl_file.endswith(".json"):
                tmpl_cfg = read_json_gcs(project_id, landing_bucket, tmpl_file)
                # delete template when existed
                delete_template(project_id, tmpl_cfg["template_id"], tmpl_cfg["location"])
                result = create_template(project_id, tmpl_cfg["template_id"], tmpl_cfg["location"], tmpl_cfg["display_name"], tmpl_cfg["fields"])
                if result:
                    move_file_gcs(project_id, landing_bucket, tmpl_file, archive_bucket, f"{template_folder}/{tmpl_file.split('/')[-1]}.done")
    return True