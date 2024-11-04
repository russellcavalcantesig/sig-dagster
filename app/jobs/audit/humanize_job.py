from dagster import job

from ...resources.resources import resource_defs
from ...ops.humanize_job import transform_data_humanized, extract_from_source_normalized, load_to_audit_humanized



@job(resource_defs=resource_defs)
def humanize_job():
    # print('entrou ->>', 'dada')

    documents = extract_from_source_normalized()
    transformed_data = transform_data_humanized(documents)
    load_to_audit_humanized(transformed_data)

