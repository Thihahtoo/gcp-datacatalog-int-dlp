from google.cloud import bigquery
import csv
from datetime import datetime
from utils.utils import read_json
from utils.tag_operation import get_tag_info
from utils.gcs_operation import upload_file_to_gcs
from utils.tmpl_operation import get_template_info, list_template

def extract_all_template_info_to_file(project_id, file_path):
    run_date = datetime.today().strftime('%Y%m%d%H%M%S')
    csv_name = f"{file_path}template_info_{run_date}.csv"

    print(f"Extracting template info to file : {csv_name}")
    with open(csv_name, 'w') as csv_file:
        # write csv header
        writer = csv.DictWriter(csv_file, ["project_id", "template_id", "template_loc", 
                "field_id", "field_display_name", "field_type", "field_allowed_values",
                "requried_field", "field_description", "extract_timestamp"])
        writer.writeheader()
        templates = list_template(project_id)
        for tmpl in templates:
            tmpl_id = tmpl.split('/')[-1]
            tmpl_loc = tmpl.split('/')[3]
            tmpl_info = get_template_info(project_id, tmpl_id, tmpl_loc)
            print(f"Writing template info for: {tmpl_loc}, {tmpl_id}")
            for row in tmpl_info:
                row["extract_timestamp"] = run_date
                writer.writerow(row)
    print("Extract finished.")
    return csv_name

def extract_all_tag_info_to_file(project_id, file_path):
    client = bigquery.Client(project = project_id)
    client.list_datasets()
    datasets = client.list_datasets(project_id)

    run_date = datetime.today().strftime('%Y%m%d%H%M%S')
    csv_name = f"{file_path}tag_info_{run_date}.csv"

    print(f"Extracting tag info to file : {csv_name}")
    with open(csv_name, 'w') as csv_file:
        # write csv header
        writer = csv.DictWriter(csv_file, ["project_id", "dataset_name", "table_name", "column_name", "template_id", 
                                            "template_location", "tag_field_id", "tag_field_value", "extract_timestamp"])
        writer.writeheader()

        for dataset in datasets:
            dataset_name = dataset.dataset_id
            # get tags for dataset level
            dataset_tag = get_tag_info(project_id, dataset_name)
            if dataset_tag:
                # write dataset tags to csv
                print(f"Writing tag info for: {dataset_name}")
                for row in dataset_tag:
                    row["extract_timestamp"] = run_date
                    writer.writerow(row)
            else:
                tmp_dict = {"project_id":project_id, "dataset_name":dataset_name, "table_name":"", "column_name":"", 
                                "template_id":"", "template_location":"",
                                "tag_field_id":"", "tag_field_value":"", "extract_timestamp":run_date}
                writer.writerow(tmp_dict)

            tables = client.list_tables(dataset_name)
            for table in tables:
                table_name = table.table_id
                # get tags for table and column level
                table_tag = get_tag_info(project_id, dataset_name, table_name)
                if table_tag:
                    # write table tags to csv
                    print(f"Writing tag info for: {dataset_name}.{table_name}")
                    for row in table_tag:
                        row["extract_timestamp"] = run_date
                        writer.writerow(row)
                else:
                    tmp_dict = {"project_id":project_id, "dataset_name":dataset_name, "table_name":table_name, 
                                    "column_name":"", "template_id":"", "template_location":"", 
                                     "tag_field_id":"", "tag_field_value":"", "extract_timestamp":run_date}
                    writer.writerow(tmp_dict)

    print("Extract finished.")
    return csv_name

def load_file_to_bigquery(project_id, file_gcs_location, destination_dataset, destination_table, schema):
    client = bigquery.Client(project=project_id)
    table_id = f"{destination_dataset}.{destination_table}"

    table_schema = []
    for key, value in schema.items():
        table_schema.append(bigquery.SchemaField(key, value))
    
    job_config = bigquery.LoadJobConfig(
        schema = table_schema,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        skip_leading_rows = 1,
        # The source format defaults to CSV, so the line below is optional.
        source_format = bigquery.SourceFormat.CSV,
    )

    load_job = client.load_table_from_uri(file_gcs_location, table_id, job_config=job_config)
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows to: {project_id}.{table_id}")

def extract_datacatalog():
    job_config = read_json("config/config.json")

    project_id = job_config["project_id"]
    bucket = job_config["extract_bucket"]
    gcs_folder = job_config["extract_folder"]
    destination_dataset = job_config["extract_destination_dataset"]

    tmpl_destination_table = job_config["template_extract_destination_table"]
    tag_destination_table = job_config["tag_extract_destination_table"]

    tmpl_table_schema = read_json(job_config["template_extract_table_schema"])
    tag_table_schema = read_json(job_config["tag_extract_table_schema"])

    # extract template info file to local directory
    if job_config["run_local"]:
        tmpl_filename = extract_all_template_info_to_file(project_id, "catalog_extract/")
    else:
        tmpl_filename = extract_all_template_info_to_file(project_id, "/tmp/")

    # extract tag info file to local directory
    if job_config["run_local"]:
        tag_filename = extract_all_tag_info_to_file(project_id, "catalog_extract/")
    else:
        tag_filename = extract_all_tag_info_to_file(project_id, "/tmp/")

    # upload extracted files to gcs
    tmpl_file_path_on_gcs = f"{gcs_folder}/{tmpl_filename.split('/')[-1]}"
    upload_file_to_gcs(project_id, bucket, tmpl_filename, tmpl_file_path_on_gcs)

    tag_file_path_on_gcs = f"{gcs_folder}/{tag_filename.split('/')[-1]}"
    upload_file_to_gcs(project_id, bucket, tag_filename, tag_file_path_on_gcs)

    # load extracted files to Bigquery
    load_file_to_bigquery(project_id, f"gs://{bucket}/{tmpl_file_path_on_gcs}", destination_dataset, 
                            tmpl_destination_table, tmpl_table_schema)

    load_file_to_bigquery(project_id, f"gs://{bucket}/{tag_file_path_on_gcs}", destination_dataset, 
                            tag_destination_table, tag_table_schema)