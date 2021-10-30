from utils.tmpl_operation import read_and_create_tag_template
from utils.tag_operation import read_and_attach_tag

from google.cloud import datacatalog

def entry_point(request):
    read_and_create_tag_template()
    read_and_attach_tag()

#entry_point("request")