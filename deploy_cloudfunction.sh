#!/bin/bash

sudo apt install zip
sudo yum install zip

mkdir ./temp
zip ./temp/Data_Catalog.zip ./config/* ./utils/* ./main.py ./requirements.txt
cd ./temp/
unzip Data_Catalog.zip
rm Data_Catalog.zip

gcloud functions deploy data_catalog \
--entry-point entry_point \
--runtime python37 \
--region europe-west2 \
--trigger-resource uki_ds_data_catalog \
--trigger-event google.storage.object.finalize

rm -rf ../temp/