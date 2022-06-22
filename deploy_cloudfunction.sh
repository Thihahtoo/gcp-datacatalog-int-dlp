#!/bin/bash

FUNCTION_NAME_1="data_catalog_create_templates_and_tags"
FUNCTION_NAME_2="data_catalog_extract_data"
FUNCTION_NAME_3="data_catalog_dlp_tag_generation"
LOCATION="europe-west2"
TRIGGER_BUCKET="uki_ds_data_catalog"
SERVICE_ACCOUNT="309116795114-compute@developer.gserviceaccount.com"

sudo apt install zip
sudo yum install zip

mkdir ./temp
zip ./temp/Data_Catalog.zip ./config/* ./utils/* ./main.py ./requirements.txt
cd ./temp/
unzip Data_Catalog.zip
rm Data_Catalog.zip

gcloud functions deploy $FUNCTION_NAME_1 \
--entry-point create_template_and_tag \
--runtime python37 \
--region $LOCATION \
--trigger-resource $TRIGGER_BUCKET \
--service-account $SERVICE_ACCOUNT \
--trigger-event google.storage.object.finalize \
--retry

gcloud functions deploy $FUNCTION_NAME_2 \
--entry-point extract_datacatalog_data \
--runtime python37 \
--region $LOCATION \
--trigger-http \
--service-account $SERVICE_ACCOUNT \
--allow-unauthenticated

gcloud functions deploy $FUNCTION_NAME_3 \
--entry-point run_dlp_job \
--runtime python37 \
--region $LOCATION \
--trigger-resource $TRIGGER_BUCKET \
--service-account $SERVICE_ACCOUNT \
--trigger-event google.storage.object.finalize \
--retry

rm -rf ../temp/