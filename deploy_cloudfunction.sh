#!/bin/bash
## to be remove and put in pre-install
sudo apt -y install zip
sudo apt -y install build-essential
sudo apt -y install jq

resource_location=`jq -r .resource_location config/config.json`
function_data_catalogs=`jq -r .function_data_catalogs config/config.json`
function_data_extract=`jq -r .function_data_extract config/config.json`
trigger_bucket=`jq -r .trigger_bucket config/config.json`
service_account=`jq -r .service_account config/config.json`

cd /root/gcp_code/catalog_code/gcp-datacatalog-int-dlp/
rm -rf ./temp/
mkdir ./temp
zip ./temp/Data_Catalog.zip ./config/* ./utils/* ./main.py ./requirements.txt
cd ./temp/
unzip Data_Catalog.zip
rm Data_Catalog.zip

## this will overwrite
gcloud functions deploy $function_data_catalogs \
--entry-point create_template_and_tag \
--runtime python37 \
--region $resource_location \
--trigger-resource $trigger_bucket \
--service-account $service_account \
--trigger-event google.storage.object.finalize \
--retry

## this will overwrite
gcloud functions deploy $function_data_extract \
--entry-point extract_datacatalog_data \
--runtime python37 \
--region $resource_location \
--trigger-http \
--service-account $service_account \
--allow-unauthenticated

rm -rf ../temp/
