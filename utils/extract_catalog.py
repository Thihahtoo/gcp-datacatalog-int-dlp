from google.cloud import bigquery
import csv
from datetime import datetime
from utils.utils import read_json
from utils.tag_operation import list_tags
from utils.gcs_operation import upload_file_to_gcs

def extract_datacatalog_to_file(project_id, file_path):
    client = bigquery.Client(project = project_id)
    client.list_datasets()
    datasets = client.list_datasets(project_id)

    run_date = datetime.today().strftime('%Y%m%d%H%M%S')
    csv_name = f"{file_path}tag_list_{run_date}.csv"

    print(f"Extracting to file : {csv_name}")
    with open(csv_name, 'w') as csv_file:
        # write csv header
        writer = csv.DictWriter(csv_file, ["project_id", "dataset_name", "table_name", "column_name", 
                                            "template_id", "template_location", "extract_timestamp"])
        writer.writeheader()

        for dataset in datasets:
            dataset_name = dataset.dataset_id
            # get tags for dataset level
            dataset_tag = list_tags(project_id, dataset_name)
            if dataset_tag:
                # write dataset tags to csv
                print(f"Writing tag info for: {dataset_name}")
                for row in dataset_tag:
                    row["extract_timestamp"] = run_date
                    writer.writerow(row)
            else:
                tmp_dict = {"project_id":project_id, "dataset_name":dataset_name, "table_name":"", "column_name":"", 
                                "template_id":"", "template_location":"", "extract_timestamp":run_date}
                writer.writerow(tmp_dict)

            tables = client.list_tables(dataset_name)
            for table in tables:
                table_name = table.table_id
                # get tags for table and column level
                table_tag = list_tags(project_id, dataset_name, table_name)
                if table_tag:
                    # write table tags to csv
                    print(f"Writing tag info for: {dataset_name}.{table_name}")
                    for row in table_tag:
                        row["extract_timestamp"] = run_date
                        writer.writerow(row)
                else:
                    tmp_dict = {"project_id":project_id, "dataset_name":dataset_name, "table_name":table_name, "column_name":"", 
                                    "template_id":"", "template_location":"", "extract_timestamp":run_date}
                    writer.writerow(tmp_dict)

    print("Extract finished.")
    return csv_name

def load_file_to_bigquery(project_id, file_gcs_location, destination_dataset, destination_table):
    client = bigquery.Client(project=project_id)
    table_id = f"{destination_dataset}.{destination_table}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("project_id", "STRING"),
            bigquery.SchemaField("dataset_name", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("column_name", "STRING"),
            bigquery.SchemaField("template_id", "STRING"),
            bigquery.SchemaField("template_location", "STRING"),
             bigquery.SchemaField("extract_timestamp", "STRING")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    load_job = client.load_table_from_uri(file_gcs_location, table_id, job_config=job_config)
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows to: {project_id}.{table_id}")

def extract_datacatlog():
    job_config = read_json("config/config.json")

    project_id = job_config["project_id"]
    bucket = job_config["extract_bucket"]
    gcs_folder = job_config["extract_folder"]
    destination_dataset = job_config["extract_destination_dataset"]
    destination_table = job_config["extract_destination_table"]

    # extract file to local directory
    if job_config["run_local"]:
        filename = extract_datacatalog_to_file(project_id, "catalog_extract/")
    else:
        filename = extract_datacatalog_to_file(project_id, "/tmp/")

    # upload extracted file to gcs
    file_path_on_gcs = f"{gcs_folder}/{filename.split('/')[-1]}"
    upload_file_to_gcs(project_id, bucket, filename, file_path_on_gcs)

    # load extracted file to Bigquery
    load_file_to_bigquery(project_id, f"gs://{bucket}/{file_path_on_gcs}", destination_dataset, destination_table)