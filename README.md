# Tagging with GCP Data Catalog

This framework is designed to leverage the functionality of GCP DataCatalog service and automate the process flow for tagging and tag template creation by running locally or deploying it to GCP Cloud Function. It includes two parts.

1. Tag Template Creation
2. Bigquery Tag Creation

Global configuration can be found under `config/config.json` which you can edit according to your requirements.

## 1. Tag Template Creation
Follow the below steps to create Tag Template:

1. Create the json file that includes template definition as following:
    ```json
    {
        "template_id": "template_demo",
        "location": "europe-west2",
        "display_name": "Demo Tag Template",
        "fields": [
            {
                "id": "source",
                "display_name": "Source of data asset",
                "type": "string",
                "required": true
            },
            {
                "id": "num_rows",
                "display_name": "Number of rows in the data asset",
                "type": "double",
                "required": false
            },
            {
                "id": "has_pii",
                "display_name": "Has PII",
                "type": "bool",
                "required": false
            },
            {
                "id": "pii_type",
                "display_name": "PII type",
                "type": "enum",
                "allowed_values": [
                    "EMAIL_ADDRESS",
                    "US_SOCIAL_SECURITY_NUMBER",
                    "NONE"
                ],
                "required": false
            }
        ]
    }
    ```
2. **To pick up file from local directory**:
    *   Set the ```"run_local": true``` in `config/config.json`.
    *   Store the json files under `tag_template/landing/` (Create as many json files as you want under this directory).

    **To pick up files from GCS location**:
    *   Set the ```"run_local": false``` in `config/config.json`.
    *   Upload the files to gcs bucket as you described in `config/config.json` which is `"template_landing_bucket" : "<bucket_name>"` under `"template_folder": "<folder_name>"`.\
    **Example**: If your template_landing_bucket name is ***test_landing*** and your template_folder name is ***template***, store your file under ***gs://test_landing/template/***.
3. After preparing json files under respective locaiton, run main python file as ```python3 main.py```.
4. The framework will pick up all the json files start with **template** from specified location and will create the the templates according to json files.
5. When it successfully created the templates, it will move these json files to `tag_template/processed/` (for local files) or `gs://<template_archive_bucket>/<template_folder>` (for files from gcs) directory and renamed them as **template_*.json.done**.
## 2. Bigquery Tag Creation
Follow the below steps to create Tag for Bigquery tables:
1. Create the csv file that includes tag information as following:

    |project_name|dataset_name|table_name|source|num_rows|has_pii|pii_type|template_id|template_location|
    |---|---|---|---|---|---|---|---|---|
    |acn-uki-ds-data-ai-project|data_catalog_dev|covid_worldwide|BigQuery|1000|FALSE||template_demo|europe-west2|
    |acn-uki-ds-data-ai-project|data_catalog_dev|covid_worldwide_new|BigQuery|1000|TRUE|EMAIL_ADDRESS|template_demo|europe-west2|

    ***Note***: **dataset_name**, **table_name**, **template_id** and **template_location** are required fields to successfully create the tag.

2.  **To pick up file from local directory**:
    *   Set the ```"run_local": true``` in `config/config.json`.
    *   Store the csv files under `tags/landing/` (Create as many csv files as you want under this directory).

    **To pick up files from GCS location**:
    *   Set the ```"run_local": false``` in `config/config.json`.
    *   Upload the files to gcs bucket as you described in `config/config.json` which is `"tag_landing_bucket" : "<bucket_name>"` under `"tag_folder": "<folder_name>"`.\
    **Example**: If your tag_landing_bucket name is ***test_landing*** and your tag_folder name is ***tag***, store your file under ***gs://test_landing/tag/***.
3. After preparing csv files under respective locaiton, run main python file as ```python3 main.py```.
4. The framework will pick up all the csv files start with from specified location and will tag to BigQuery tables according to csv files.
5. When it successfully tagged the tables, it will move these csv files to `tag/processed/` (for local files) or `gs://<tag_archive_bucket>/<tag_folder>` (for files from gcs) directory and renamed them as ***.csv.done**.

## To Deploy The Framework to GCP Cloud Function
Simply run deploy_cloudfunction.sh as `./deploy_cloudfunction.sh`. Edit the cloud function name, function location and trigger bucket name according to your needs.\
**Example**:
```sh
FUNCTION_NAME="<your_function_name>"
LOCATION="<your_function_location>"
TRIGGER_BUCKET="<your_bucket_name>"
```

## After Cloud Function is deployed
Upload the files to your landing buckets.
*   ### For template json files
    `gs://<template_landing_bucket>/<template_folder>/`
*   ### For tag csv files
    `gs://<tag_landing_bucket>/<tag_folder>/`

After uploaded these files, the cloud function will pick up the files automatically within a few seconds and move the completed files to archived buckets.