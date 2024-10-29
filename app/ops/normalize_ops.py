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
    # Você pode adicionar transformações aqui, se necessário
    return documents

@op(required_resource_keys={'mongodb'})
def load_to_audit_state(context, documents):
    db = context.resources.mongodb['admin']
    audit_collection = db['AuditState']
    raw_collection = db['AuditRaw']
    
    if documents:
        # Inserir documentos na coleção AuditState
        result = audit_collection.insert_many(documents)
        context.log.info(f"Inserted {len(result.inserted_ids)} documents into AuditState collection")
        
        # Remover documentos da coleção AuditRaw após inseri-los em AuditState
        document_ids = [doc['_id'] for doc in documents]  # Pega os IDs dos documentos
        delete_result = raw_collection.delete_many({'_id': {'$in': document_ids}})
        context.log.info(f"Deleted {delete_result.deleted_count} documents from AuditRaw collection")
    else:
        context.log.info("No documents to insert or delete")
