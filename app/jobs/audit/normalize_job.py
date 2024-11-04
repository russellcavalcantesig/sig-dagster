from dagster import job

from ...resources.resources import resource_defs
from ...ops.normalize_ops import transform_data, transform_data_state ,extract_from_source, load_to_audit_normalized, load_to_audit_state



@job(resource_defs=resource_defs)
def audit_normalize_job():
    documents = extract_from_source()
    
    transformed_data_state = transform_data_state(documents)
    # print('transformed_data_state --->>>',transformed_data_state)
    
    
    
    load_to_audit_state(transformed_data_state)
    # print('loaded_audit_state --->>.',loaded_audit_state)
    # input()
    transformed_data = transform_data(documents)
    
    
    load_to_audit_normalized(transformed_data)
    

