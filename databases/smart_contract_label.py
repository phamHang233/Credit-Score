from pymongo import MongoClient

from config import SmartContractLabel


class LabelMongoDb:
    def __init__(self, graph=None):
        if not graph:
            graph = SmartContractLabel.CONNECTION_URL

        self.connection_url = graph.split("@")[-1]
        self.connection = MongoClient(graph)
        self.mongo_db = self.connection[SmartContractLabel.DATABASE]

        self.protocols = self.mongo_db["protocols"]

    def get_all_doc(self):
        return self.protocols.find({})

    def get_all_protocol_by_chain(self, chain_id):
        docs = self.protocols.find({
            '_id': {
                '$regex': f'{chain_id}_'
                }
            })
        return docs
