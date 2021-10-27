import subprocess
import json
import csv, re

def run_shell_cmd(cmd):
    return subprocess.run(cmd, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def read_json(file_name):
    with open(file_name, 'r') as f:
        template = json.load(f)
    return template

def read_tag_csv(file_name):
    result = []
    with open(file_name, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row_dict = {}
            row_dict['project_name'] = row['project_name']
            row_dict['dataset_name'] = row['dataset_name']
            row_dict['table_name'] = row['table_name']
            row_dict['template_id'] = row['template_id']
            row_dict['template_location'] = row['template_location']
            # remove unnecessary fields to create tag json file
            row.pop('template_id')
            row.pop('template_location')
            row_dict['tag_json'] = row
            result.append(row_dict)
    return result

def dict_to_json(dict_obj ,filename):
    with open(filename, "w") as outfile:
        json.dump(dict_obj, outfile)
    return True

def prepare_dict(dict_obj):
    for key, value in list(dict_obj.items()):
        if value.lower() == "true":
            dict_obj[key] = True
        if value.lower() == "false":
            dict_obj[key] = False
        if re.fullmatch('[0-9]+',value):
            dict_obj[key] = int(value)
        if value == '':
            dict_obj.pop(key)
    return dict_obj
