from pymongo import MongoClient
from config import MongoConfig
from utils.logger_utils import get_logger

logger = get_logger("Blockchain ETL")


class MongoDB:
    def __init__(self, connection_url=None, db_prefix=""):
        self._conn = None
        if not connection_url:
            connection_url = MongoConfig.CONNECTION_URL

        _db = MongoConfig.DATABASE
        # if databases:
        #     _db = databases
        self.connection = MongoClient(connection_url)
        if db_prefix:
            db_name = db_prefix + "_" + _db
        else:
            db_name = _db

        self.mongo_db = self.connection[db_name]
        self.transaction = self.mongo_db['transactions']
        self.blocks= self.mongo_db['blocks']
        self.projects= self.mongo_db['projects']

    def get_smart_contracts(self):
        cursor = self.mongo_db.find({""}, batch_size=10000)
        smart_contracts = []
        for smart_contract in cursor:
            smart_contracts.append(smart_contract)

        return smart_contracts

    #     def get_document(self, collection, conditions, args=None):
    #         _collection = self.mongo_db[collection]
    #         if args:
    #             result = _collection.find_one(conditions, args)
    #         else:
    #             result = _collection.find_one(conditions)
    #         return result

    def get_documents(self, collection, conditions, args=None):
        _collection = self.mongo_db[collection]
        if args:
            result = _collection.find(conditions, args)
        else:
            result = _collection.find(conditions)
        return result

    def get_transactions(self, filter, args = None):
        if args:
            result = self.transaction.find(filter, args)
        else:
            result = self.transaction.find(filter)
        return result

    def get_transactions_count_doc(self, filter):
        number = self.transaction.count_documents(filter)
        return number

#     @retry_handler
#     def update_document(self, collection, document, upsert=True):
#         _collection = self.mongo_db[collection]
#         try:
#             _collection.update({"_id": document["_id"]}, {"$set": document}, upsert=upsert)
#             success = True
#         except Exception as e:
#             logger.error(e)
#             success = False

#         return success

#     @retry_handler
#     def update_documents(self, collection, documents, upsert=True):
#         _collection = self.mongo_db[collection]
#         try:
#             bulk_operations = [UpdateOne({'_id': document['_id']}, {"$set": flatten_dict(document)}, upsert=upsert)
#                                for document in documents]
#             _collection.bulk_write(bulk_operations)
#             success = True
#         except Exception as e:
#             logger.error(e)
#             success = False

#         return success


# def flatten_dict(d):
#     out = {}
#     for key, val in d.items():
#         if isinstance(val, dict):
#             val = [val]
#         if isinstance(val, list):
#             for subdict in val:
#                 deeper = flatten_dict(subdict).items()
#                 out.update({key + '.' + key2: val2 for key2, val2 in deeper})
#         else:
#             out[key] = val
#     return out
    def get_address_in_projects(self, protocols, args=None):
        if args:
            return self.projects.find_one({"_id": protocols }, args)
        else:
            return self.projects.find_one({"_id": protocols})
