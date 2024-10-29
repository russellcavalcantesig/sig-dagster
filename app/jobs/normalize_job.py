from dagster import job

from ..resources.resources import resource_defs
from ..ops.normalize_ops import transform_data, extract_from_source, load_to_audit_state



@job(resource_defs=resource_defs)
def mongo_audit_job():
    documents = extract_from_source()
    transformed_data = transform_data(documents)
    load_to_audit_state(transformed_data)

