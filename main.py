import enum
from utils.tmpl_operation import read_and_create_tag_template, get_latest_template_id
from utils.tag_operation import read_and_attach_tag
from google.cloud import datacatalog


def entry_point(request1, request2):
    read_and_create_tag_template()
    read_and_attach_tag()

entry_point("request1", "request2")