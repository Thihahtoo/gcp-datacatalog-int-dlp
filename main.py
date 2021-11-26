from utils.tmpl_operation import create_tag_template_from_file
from utils.tag_operation import read_and_attach_tag
from utils.extract_catalog import extract_datacatalog

def create_template_and_tag(request1, request2):
    create_tag_template_from_file()
    read_and_attach_tag()
    # create_taxonomy_from_file()
    
def extract_datacatalog_data(request1):
    extract_datacatalog()

create_template_and_tag("request1", "request2")
extract_datacatalog_data("request1")