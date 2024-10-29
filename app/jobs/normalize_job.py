from dagster import job

from ..resources.resources import mongodb_resource

from dagster import op

@op(required_resource_keys={'mongodb'})
def extract_from_source(context):
    db = context.resources.mongodb['admin']
    collection = db['AuditRaw']
    
    documents = list(collection.find({}))
    context.log.info(f"Extracted {len(documents)} documents from source collection")
    return documents

@op
def transform_data(documents):
    return documents

@op(required_resource_keys={'mongodb'})
def load_to_audit_state(context, documents):
    db = context.resources.mongodb['admin']
    audit_collection = db['AuditState']
    
    if documents:
        result = audit_collection.insert_many(documents)
        context.log.info(f"Inserted {len(result.inserted_ids)} documents into AuditState collection")
    else:
        context.log.info("No documents to insert")


@job(resource_defs={'mongodb': mongodb_resource})
def mongo_audit_job():
    documents = extract_from_source()
    transformed_data = transform_data(documents)
    load_to_audit_state(transformed_data)

