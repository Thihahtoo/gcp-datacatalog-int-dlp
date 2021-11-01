#!/bin/bash

FUNCTION_NAME="data_catalog"
LOCATION="europe-west2"
TRIGGER_BUCKET="uki_ds_data_catalog"

sudo apt install zip
sudo yum install zip

mkdir ./temp
zip ./temp/Data_Catalog.zip ./config/* ./utils/* ./main.py ./requirements.txt
cd ./temp/
unzip Data_Catalog.zip
rm Data_Catalog.zip

gcloud functions deploy $FUNCTION_NAME \
--entry-point entry_point \
--runtime python37 \
--region $LOCATION \
--trigger-resource $TRIGGER_BUCKET \
--trigger-event google.storage.object.finalize

rm -rf ../temp/