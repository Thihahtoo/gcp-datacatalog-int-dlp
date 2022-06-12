import subprocess
import json
import csv, re

def run_shell_cmd(cmd,incwd):
    return subprocess.run(cmd, cwd=incwd, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def read_json(file_name):
    with open(file_name, 'r') as f:
        template = json.load(f)
    return template

def read_tag_csv(file_name):
    result = []
    with open(file_name, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            result.append(row)
    return result

def dict_to_json(dict_obj ,filename):
    with open(filename, "w") as outfile:
        json.dump(dict_obj, outfile)
    return True

def prepare_dict(dict_obj):
    result_obj = dict_obj.copy()
    for key, value in list(result_obj.items()):
        if value.lower() == "true":
            result_obj[key] = True
        if value.lower() == "false":
            result_obj[key] = False
        if re.fullmatch('[0-9]+',value):
            result_obj[key] = int(value)
        if value == '':
            result_obj.pop(key)
    return result_obj
