from pymongo import MongoClient
from dagster import resource

@resource
def mongodb_resource():
    # client = MongoClient('mongodb://localhost:27017/')
    client = MongoClient('mongodb://192.168.17.106:27017/')
    return client

resource_defs = {
        "mongodb": mongodb_resource
    }