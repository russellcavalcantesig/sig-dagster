from dagster import job

from ...resources.resources import resource_defs
from ...ops.humanize_job import extract_from_normalized, process_documents, cleanup_normalized



@job(resource_defs=resource_defs)
def humanize_job():
    # print('entrou ->>', 'dada')

    docs = extract_from_normalized()
    processed = process_documents(docs)
    cleanup_normalized(processed)

