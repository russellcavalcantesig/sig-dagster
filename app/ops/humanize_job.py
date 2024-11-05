from dagster import op, job
from typing import Dict, Any, List
from datetime import datetime
from copy import deepcopy

@op(required_resource_keys={'mongodb'})
def extract_from_normalized(context):
    """Extract documents from AuditNormalize collection."""
    db = context.resources.mongodb['mvp']
    collection = db['AuditNormalized']
    documents = list(collection.find({}))
    print('documents ->>>',documents)
    context.log.info(f"Extracted {len(documents)} documents from AuditNormalized")
    return documents


@op(required_resource_keys={'mongodb'})
def process_documents(context, documents: List[Dict[str, Any]]):
    """Process documents and validate fields against AuditMapper."""
    db = context.resources.mongodb['mvp']
    normalized_collection = db['AuditNormalized']
    audit_mapper = db['AuditMapper']
    audit_inconsistence = db['AuditInconsistence']
    processed_docs = []
    
    for doc in documents:
        try:
            # 1. Extract fields from document
            document_fields = doc.get('document', {})
            schema = doc.get('schema')
            table = doc.get('table')
            
            has_inconsistency = False

        
            
            # 2. Verify fields in AuditMapper
            for field_name, field_value in document_fields.items():
                mapper_query = {
                    'schema': schema,
                    'table': table,
                    'field': field_name
                }
                
                mapper_doc = audit_mapper.find_one(mapper_query)

                if not mapper_doc or mapper_doc['status'] != 3:
                    has_inconsistency = True

                # 2.1 If field not found in AuditMapper, create new record
                if not mapper_doc:

                    new_mapper_doc = {
                        'schema': schema,
                        'table': table,
                        'field': field_name,
                        'label': None,
                        'status': 0,
                        'type': None,
                        'query': None,
                        'error': 'Campo não mapeado',
                        'createdAt': datetime.now()
                    }
                    audit_mapper.insert_one(new_mapper_doc)
                    print(f'Created new mapper record for {schema}.{table}.{field_name}')
                    # input()
                    context.log.info(f"Created new mapper record for {schema}.{table}.{field_name}")
                    
                    
            
            # 3. If any inconsistency found, move document to AuditInconsistence
            if has_inconsistency:
                doc['movedAt'] = datetime.now()
                audit_inconsistence.insert_one(doc)
                context.log.info(f"Moved document {doc['_id']} to AuditInconsistence")
            else:
                processed_docs.append(doc)

            # Remover documentos processados da coleção Normalize Colection
            document_ids = [doc['_id'] for doc in documents]
            delete_result = normalized_collection.delete_many({'_id': {'$in': document_ids}})
            context.log.info(f"Deleted {delete_result.deleted_count} documents from AuditRaw collection")
        except Exception as e:
            context.log.error(f"Error processing document {doc.get('_id')}: {str(e)}")
            continue
            
    return processed_docs

@op(required_resource_keys={'mongodb'})
def cleanup_normalized(context, processed_docs: List[Dict[str, Any]]):
    """Remove processed documents from AuditNormalize."""
    if not processed_docs:
        return
        
    db = context.resources.mongodb['mvp']
    normalize_collection = db['AuditNormalize']
    
    doc_ids = [doc['_id'] for doc in processed_docs]
    result = normalize_collection.delete_many({'_id': {'$in': doc_ids}})
    
    context.log.info(f"Removed {result.deleted_count} processed documents from AuditNormalize")

