from dagster import job, op, resource
from pymongo import MongoClient
from datetime import datetime

# Resource definition for MongoDB connection
@resource
def mongodb_resource():
    client = MongoClient('mongodb://localhost:27017/')
    return client

# Operation to read from source collection
@op(required_resource_keys={'mongodb'})
def extract_from_source(context):
    db = context.resources.mongodb['admin']
    collection = db['AuditRaw']
    
    # Get all documents from source collection
    documents = list(collection.find({}))
    context.log.info(f"Extracted {len(documents)} documents from source collection")
    return documents

# Operation to transform data if needed
@op
def transform_data(documents):
    # In this case, we're not transforming the data
    # but you can add any transformation logic here if needed
    return documents

# Operation to load data into AuditState collection
@op(required_resource_keys={'mongodb'})
def load_to_audit_state(context, documents):
    db = context.resources.mongodb['admin']
    audit_collection = db['AuditState']
    
    # Insert documents into AuditState collection
    if documents:
        result = audit_collection.insert_many(documents)
        context.log.info(f"Inserted {len(result.inserted_ids)} documents into AuditState collection")
    else:
        context.log.info("No documents to insert")

# Define the job
@job(resource_defs={'mongodb': mongodb_resource})
def mongo_audit_job():
    documents = extract_from_source()
    transformed_data = transform_data(documents)
    load_to_audit_state(transformed_data)

# If you want to execute the job directly (for testing)
if __name__ == "__main__":
    result = mongo_audit_job.execute_in_process()