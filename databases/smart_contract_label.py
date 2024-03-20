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
        self.smart_contract = self.mongo_db['smart_contracts']

    def get_all_doc(self):
        return self.protocols.find({})

    def get_all_protocol_by_chain(self, chain_id):
        docs = self.protocols.find({
            '_id': {
                '$regex': f'{chain_id}_'
            }
        })
        return docs

    def get_smart_contract_by_project(self, project, chain_id):
        filter = {
            'project': {
                '$regex': project
            },
            'chainId': chain_id
        }
        docs = self.smart_contract.find(filter)
        return docs

    def get_docs_by_forked(self, forked, chain_id: str = None):
        filter = {
            '_id': {
                '$regex': chain_id
            },
            'forked': {
                '$regex': forked
            }}
        docs = self.protocols.find(filter)

        return docs
