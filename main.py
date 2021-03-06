from utils.tmpl_operation import create_tag_template_from_file
from utils.tag_operation import read_and_attach_tag
from utils.extract_catalog import extract_datacatalog
from utils.taxonomy_operation import create_taxonomy_from_file
from utils.policy_tag_operation import read_and_attach_policy_tag, auto_attach_policy_tag
# from utils.dlp_operation import extract_dlp_config

def create_template_and_tag(request1, request2):
    create_tag_template_from_file()
    read_and_attach_tag()
    create_taxonomy_from_file()
    read_and_attach_policy_tag()
    
def extract_datacatalog_data(request1):
    extract_datacatalog()


# create_template_and_tag("request1", "request2")
# extract_datacatalog_data("request1")
