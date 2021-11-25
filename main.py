from utils.tmpl_operation import create_tag_template_from_file
from utils.tag_operation import read_and_attach_tag
from utils.extract_catalog import extract_datacatlog

def entry_point(request1, request2):
    create_tag_template_from_file()
    read_and_attach_tag()
    extract_datacatlog()

entry_point("request1", "request2")