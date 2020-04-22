from pymongo import MongoClient


def connect(index, collection):

    client = MongoClient()
    client = MongoClient('localhost', 27017)

    index_db = client.get_database(index)

    collection = index_db.get_collection(collection)

    return collection