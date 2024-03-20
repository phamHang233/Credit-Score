from databases.smart_contract_label import LabelMongoDb
from databases.mongodb import MongoDB as ETL
import threading


class ProcessChainThread():
    def __init__(self, chain_id, chain_name, block_number):
        # threading.Thread.__init__(self)
        self.chain_id = chain_id
        self.chain_name = chain_name
        self.etl_mongodb = ETL(db_prefix=self.chain_name)
        self.block_number = block_number
    def run(self):
        label_mongodb = LabelMongoDb()

        docs = label_mongodb.get_smart_contract_by_project('aave', self.chain_id)
        address = [doc['address'] for doc in docs]

        docs = label_mongodb.get_docs_by_forked('aave', self.chain_id)
        for doc in docs:
            for reserve_token, token_info in doc['reservesList'].items():
                t_token = token_info.get('tToken')
                d_token = token_info.get('dToken')
                address.append(t_token)
                address.append(d_token)

        address = list(set(address))
        numbers = self.etl_mongodb.transaction.count_documents({'to_address': {'$in': address}, 'block_number': {'$gt': self.block_number}})
        print(f'Number of transactions in {self.chain_id} is: {numbers}')

if __name__ == "__main__":
    chains = {
        "0x1": "ethereum",
        "0x38": "",
        "0xfa": "ftm",
        "0xa": "optimism",
        "0xa4b1": "arbitrum",
        "0xa86a": "avalanche",
        "0x89": "polygon",
    }

    threads = []
    # for chain_id, chain_name in chains.items():
    thread = ProcessChainThread('0x89', 'polygon',)
    thread.run()

    # Start all threads
    # for thread in threads:
    #     thread.start()
    #
    # # Wait for all threads to finish
    # for thread in threads:
    #     thread.join()
