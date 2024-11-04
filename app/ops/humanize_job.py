from dagster import op
from typing import Dict, Any
from copy import deepcopy

@op
def transform_data_humanized(documents):
    # Você pode adicionar transformações aqui, se necessário
    return documents

@op(required_resource_keys={'mongodb'})
def extract_from_source_normalized(context):
    db = context.resources.mongodb['admin']
    collection = db['AuditNormalized']
    
    documents = list(collection.find({}))
    context.log.info(f"Extracted {len(documents)} documents from source collection")
    # print(documents)
    # input()
    return documents


def get_document_differences_humanized(new_doc: Dict[str, Any], existing_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compara dois documentos e retorna um dicionário com as diferenças encontradas.
    """
    differences = {}
    
    # Campos principais que queremos sempre manter atualizados
    main_fields = ['action', 'schema', 'table']
    for field in main_fields:
        if new_doc.get(field) != existing_doc.get(field):
            differences[field] = new_doc[field]
    
    # Compara os campos dentro do documento
    new_document = new_doc.get('document', {})
    existing_document = existing_doc.get('document', {})
    
    if new_document != existing_document:
        differences['document'] = new_document
    
    # print(differences)
    # input()
    
    
    return differences

@op(required_resource_keys={'mongodb'})
def load_to_audit_humanized(context, documents):
    db = context.resources.mongodb['admin']
    normalized_collection = db['AuditNormalized']
    humanized_collection = db['AuditHumanized']
    
    if documents:
        updates = 0
        inserts = 0
        skipped = 0
        
        for doc in documents:
            try:
                # Definir a chave composta
                # composite_key = {
                #     'schema': doc['schema'],
                #     'table': doc['table'],
                #     'id': doc['id']
                # }
                
                # # Busca documento existente usando a chave composta
                # existing_doc = humanized_collection.find_one(composite_key)
                
                # # print('existing_doc -->>', existing_doc)
                # # input()
                # # Remove _id do documento a ser processado
                # new_doc = deepcopy(doc)
                # if '_id' in new_doc:
                #     del new_doc['_id']
                
                # if existing_doc:
                #     # Se o documento já existe, atualizamos independente da action
                #     differences = get_document_differences_humanized(new_doc, existing_doc)
                    
                #     if differences:
                #         update_result = humanized_collection.update_one(
                #             composite_key,
                #             {'$set': differences}
                #         )
                        
                #         if update_result.modified_count > 0:
                #             updates += 1
                #             context.log.info(
                #                 f"Updated document for {doc['schema']}.{doc['table']} "
                #                 f"id {doc['id']} with changes: {differences}"
                #             )
                #         else:
                #             skipped += 1
                #             context.log.info(
                #                 f"No changes needed for {doc['schema']}.{doc['table']} "
                #                 f"id {doc['id']}"
                #             )
                # else:
                    # Se o documento não existe, inserimos
                    humanized_collection.insert_one(doc)
                    inserts += 1
                    context.log.info(
                        f"Inserted new document for {doc['schema']}.{doc['table']} "
                        f"id {doc['id']}"
                    )
                
            except Exception as e:
                context.log.error(f"Error processing document {doc.get('schema')}.{doc.get('table')} "
                                f"id {doc.get('id')}: {str(e)}")
                continue
        
        context.log.info(
            f"Processing complete: {inserts} insertions, {updates} updates, "
            f"{skipped} skipped (no changes needed)"
        )
        
        # Remover documentos processados da coleção AuditRaw
        document_ids = [doc['_id'] for doc in documents]
        delete_result = normalized_collection.delete_many({'_id': {'$in': document_ids}})
        context.log.info(f"Deleted {delete_result.deleted_count} documents from AuditRaw collection")
    else:
        context.log.info("No documents to process")
