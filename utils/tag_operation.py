from .utils import run_shell_cmd, read_json
from .gcs_operation import list_file_gcs, download_file_gcs, move_file_gcs
import os

def get_table_entry(project, dataset, table):
    # retrieve a project.dataset.table entry
    gdc_get_table_entry = """(gcloud data-catalog entries lookup '{}' --format="value(name)")"""

    print(f'table: {project}.{dataset}.{table}')
    object_to_search = f'bigquery.table.`{project}`.{dataset}.{table}'

    gdc_cmd_table_entry = gdc_get_table_entry.format(object_to_search)
    result = run_shell_cmd(gdc_cmd_table_entry).stdout[:-1]
    return result

def remove_table_tag(table_entry, project, template, template_location):
    # remove tag for a table
    gdc_tag_list = f"""gcloud data-catalog tags list --entry={table_entry} --filter="template:projects/{project}/locations/{template_location}/tagTemplates/{template}" --format='value(name)'"""
    gdc_tag_remove = """gcloud data-catalog tags delete {} --entry={} --quiet --project={}"""
    gdc_tag_result = run_shell_cmd(gdc_tag_list)
    gdc_tag_result = gdc_tag_result.stdout[:-1]
    
    if gdc_tag_result != '':
        result = run_shell_cmd(gdc_tag_remove.format(gdc_tag_result, table_entry, project))
        print(result.stderr)
        return result.returncode
    else:
        return 1

def attach_table_tag(project, dataset, table, template, template_location, json_file):
    # create a tag for a table 
    gdc_tag_create = """gcloud data-catalog tags create --entry={} --project={} --tag-template={} --tag-template-location={} --tag-file={}"""
    table_entry = get_table_entry(project, dataset, table)
    remove_table_tag(table_entry, project, template, template_location)
    gdc_tag_create = gdc_tag_create.format(table_entry, project, template, template_location, json_file)
    result = run_shell_cmd(gdc_tag_create)

    print(result.stderr)
    return result.returncode

def read_and_attach_tag():
    job_config = read_json("config/config.json")

    project_id = job_config["project_id"]
    bucket = job_config["bucket"]
    landing = job_config["tag_landing_folder"]
    processed = job_config["tag_processed_folder"]

    if job_config["run_local"]:
        for tag_file in os.listdir("tags/landing/"):
            if tag_file.endswith(".json"):
                tag_info = tag_file.split(".")
                dataset = tag_info[0]
                table = tag_info[1]
                template = tag_info[2]
                tmplt_loc = tag_info[3]
                result = attach_table_tag(project_id, dataset, table, template, tmplt_loc, f"tags/landing/{tag_file}")
                if result == 0:
                    os.rename(f"tags/landing/{tag_file}", f"tags/processed/{tag_file}.done")
    else:
        gcs_list = list_file_gcs(project_id, bucket, f"{landing}/")
        for tag_file in gcs_list:
            if tag_file.endswith(".json"):
                tag_info = tag_file.split(".")
                dataset = tag_info[0].split("/")[-1]
                table = tag_info[1]
                template = tag_info[2]
                tmplt_loc = tag_info[3]
                download_file_gcs(project_id, bucket, tag_file, f"./{tag_file.split('/')[-1]}")
                result = attach_table_tag(project_id, dataset, table, template, tmplt_loc, f"./{tag_file.split('/')[-1]}")
                if result == 0:
                    move_file_gcs(project_id, bucket, tag_file, bucket, f"{processed}/{tag_file.split('/')[-1]}.done")
                os.remove(f"./{tag_file.split('/')[-1]}")
    return True