from dagster import op
from typing import Dict, Any
from copy import deepcopy
import json

import pika

def send_to_event_trigger(job_name: str, body:str):
    connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.17.106'))
    channel = connection.channel()
    data_dict = eval(body)
    data = json.dumps(data_dict, ensure_ascii=False)
    
    channel.queue_declare(queue='EventTrigger', durable=True)
    channel.basic_publish(exchange='', routing_key='EventTrigger', body=data, )
    print(f" [x] Enviado pelo job de ->{job_name},\n doc -> {body}, \n'RabbitMQ!'")
    connection.close()

def normalize_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove campos de auditoria do documento e retorna uma versão normalizada.
    """
    # Campos para remover do documento interno
    audit_fields = {
        'id', 
        'createdBy', 
        'createdAt', 
        'updatedBy', 
        'updatedAt', 
        'removedAt'
    }
    
    normalized_doc = deepcopy(doc)
    

    # Se existe o campo document, normaliza ele
    if 'document' in normalized_doc:
        document = normalized_doc['document']
        normalized_document = {
            k: v for k, v in document.items() 
            if k not in audit_fields
        }
        normalized_doc['document'] = normalized_document
        
        
    return normalized_doc

def get_document_differences_state(new_doc: Dict[str, Any], existing_doc: Dict[str, Any]) -> Dict[str, Any]:
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
    
    return differences

@op
def transform_data(documents):
    return [normalize_document(doc) for doc in documents]



def get_document_differences_normalize(new_doc: Dict[str, Any], existing_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compara dois documentos e retorna um dicionário com as diferenças encontradas.
    """
    differences = {}
    
    # Campos principais que queremos sempre manter atualizados
    main_fields = ['action', 'createdBy', 'createdAt']
    for field in main_fields:
        if new_doc.get(field) != existing_doc.get(field):
            differences[field] = new_doc[field]
    
    # Compara os campos dentro do documento
    new_document = new_doc.get('document', {})
    existing_document = existing_doc.get('document', {})
    
    if new_document != existing_document:
        differences['document'] = new_document
    
    
  
    return differences

@op(required_resource_keys={'mongodb'})
def extract_from_source(context):
    db = context.resources.mongodb['mvp']
    collection = db['AuditRaw']
    
    documents = list(collection.find({}))
    context.log.info(f"Extracted {len(documents)} documents from source collection")
    
    print('documents ->>>',documents)
    return documents

@op
def transform_data_state(documents):
    # Você pode adicionar transformações aqui, se necessário
    return documents

@op(required_resource_keys={'mongodb'})
def load_to_audit_state(context, documents):
    db = context.resources.mongodb['mvp']
    audit_collection = db['AuditState']
    raw_collection = db['AuditRaw']
    
    
    if documents:
        updates = 0
        inserts = 0
        skipped = 0
            
        documents_new = []
        
        
        
        for doc in documents:
            try:
                # Definir a chave composta
                composite_key = {
                    'schema': doc['schema'],
                    'table': doc['table'],
                    'id': doc['id']
                }
                
                print('doc-->>',doc)
                # input()
                
                # Busca documento existente usando a chave composta
                existing_doc = audit_collection.find_one(composite_key)
                
                # Remove _id do documento a ser processado
                new_doc = deepcopy(doc)
                if '_id' in new_doc:
                    del new_doc['_id']
                
                if existing_doc:
                    # Se o documento já existe, atualizamos independente da action
                    differences = get_document_differences_state(new_doc, existing_doc)
                    
                    if differences:
                        update_result = audit_collection.update_one(
                            composite_key,
                            {'$set': differences}
                        )
                        
                        if update_result.modified_count > 0:
                            updates += 1
                            context.log.info(
                                f"Updated document for {doc['schema']}.{doc['table']} "
                                f"id {doc['id']} with changes: {differences}"
                            )
                        else:
                            skipped += 1
                            context.log.info(
                                f"No changes needed for {doc['schema']}.{doc['table']} "
                                f"id {doc['id']}"
                            )
                    existing_doc = audit_collection.find_one(composite_key)
                    documents_new.append(existing_doc)
                else:
                    # Se o documento não existe, inserimos
                    audit_collection.insert_one(new_doc)
                    inserts += 1
                    context.log.info(
                        f"Inserted new document for {doc['schema']}.{doc['table']} "
                        f"id {doc['id']}"
                    )
                    documents_new.append(new_doc)
                
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
        delete_result = raw_collection.delete_many({'_id': {'$in': document_ids}})
        context.log.info(f"Deleted {delete_result.deleted_count} documents from AuditRaw collection")
        return documents_new
    else:
        context.log.info("No documents to process")

        
@op(required_resource_keys={'mongodb'})
def load_to_audit_normalized(context, documents):
    db = context.resources.mongodb['mvp']
    audit_normalized = db['AuditNormalized']
    audit_state = db['AuditState']
    raw_collection = db['AuditRaw']
    
    if documents:
        inserts = 0
        skipped = 0
        
        for doc in documents:
            try:
                # Definir a chave composta
                composite_key = {
                    'schema': doc['schema'],
                    'table': doc['table'],
                    'id': doc['id']
                }
                
                # Busca documento existente na State usando a chave composta
                existing_state_doc = audit_state.find_one(composite_key)
                
                # Remove _id do documento a ser processado
                new_doc = deepcopy(doc)
                if '_id' in new_doc:
                    del new_doc['_id']
                
                # Normaliza o documento removendo campos de auditoria
                normalized_doc = normalize_document(new_doc)
                
                if existing_state_doc:
                    # Encontra as diferenças entre os documentos
                    differences = {}
                    
                    # Mantém os campos principais
                    main_fields = ['action', 'schema', 'table', 'id', 'createdBy', 'createdAt']
                    for field in main_fields:
                        if field in normalized_doc:
                            differences[field] = normalized_doc[field]
                    
                    # Compara os campos dentro do document para encontrar diferenças
                    new_document = normalized_doc.get('document', {})
                    existing_document = existing_state_doc.get('document', {})
                    
                    document_differences = {}
                    for key, value in new_document.items():
                        if key in existing_document:
                            if value != existing_document[key]:
                                document_differences[key] = value
                        if key not in existing_document:
                            document_differences[key] = value
                            
                    if document_differences:
                        differences['document'] = document_differences
                        send_to_event_trigger("laod_to_audit_normalized", differences)
                        print('vai fazer o send to event trigger !')
                        input()
                        # Inserir novo documento com apenas as diferenças
                        audit_normalized.insert_one(differences)
                        inserts += 1
                        context.log.info(
                            f"Inserted normalized document with differences for {doc['schema']}.{doc['table']} "
                            f"id {doc['id']}"
                        )
                        
                        
                    else:
                        skipped += 1
                        context.log.info(
                            f"No differences found for {doc['schema']}.{doc['table']} "
                            f"id {doc['id']}"
                        )
                else:
                    # Se não existe na state, insere o documento normalizado completo
                    audit_normalized.insert_one(normalized_doc)
                    inserts += 1
                    context.log.info(
                        f"Inserted new normalized document for {doc['schema']}.{doc['table']} "
                        f"id {doc['id']}"
                    )
                
            except Exception as e:
                print(e)
                input()
                context.log.error(f"Error processing document {doc.get('schema')}.{doc.get('table')} "
                                f"id {doc.get('id')}: {str(e)}")
                # continue
        
        context.log.info(
            f"Normalization complete: {inserts} insertions, "
            f"{skipped} skipped (no changes)"
        )
        
        # Remover documentos processados da coleção AuditRaw
        document_ids = [doc['_id'] for doc in documents]
        delete_result = raw_collection.delete_many({'_id': {'$in': document_ids}})
        context.log.info(f"Deleted {delete_result.deleted_count} documents from AuditRaw collection")
    else:
        context.log.info("No documents to process")