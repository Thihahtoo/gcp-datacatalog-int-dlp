import subprocess
import json

def run_shell_cmd(cmd):
    return subprocess.run(cmd, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def read_json(file_name):
    with open(file_name, 'r') as f:
        template = json.load(f)
    return template