import threading

from databases.cs_mongodb_klg import MongoDB as KLG
from databases.mongodb import MongoDB as ETL

mongo_klg = KLG()


def process_transaction(db_prefix, start_block):
    mongo_etl = ETL(db_prefix=db_prefix)
    cursors = mongo_etl.get_address_in_projects(protocol, ['addresses'])
    contract_address = list(cursors['addresses'].keys())

    local_transaction_count = mongo_etl.get_transactions_count_doc({
        'block_number': {
            '$gte': start_block
        },
        'to_address': {'$in': contract_address}
    })
    print(f'get {local_transaction_count} contract count  in {db_prefix}')
    # with lock:
    #     global transaction_count
    #     transaction_count += local_transaction_count


# def process_active_user( chain_id, contract_address):
#     local_active_user = mongo_klg.count_wallet_interaction({
#         'lastInteractionAt': {
#             '$gte': start_timestamp
#         },
#         'contractAddress': {'$in': contract_address},
#         'chainId': chain_id
#     })
#     print(f'get {local_active_user} active user count in {chain_id}')
#
#     with lock:  # Acquire lock for thread-safe updates
#         global active_user
#         active_user += local_active_user


if __name__ == "__main__":
    protocol = 'aave'

    process_transaction(db_prefix='avalanche', start_block=41427680)
    # start_block = 19176747
    # start_timestamp = 1707363122

    #178046994
    #
    # db_prefixs = {'ethereum': '0x1',  'arbitrum': '0xa4b1', 'avalanche':'0xa86a', 'ftm': '0xfa', 'optimism': '0xa', 'polygon':'0x89'}
    # transaction_count = 0
    # active_user = 0
    # lock = threading.Lock()  # Shared lock for thread-safe updates
    #
    # threads = []
    # for db_prefix, chain_id in db_prefixs.items():
    # # for db_prefix in ['polygon']:
    #     mongo_etl = ETL(db_prefix=db_prefix)
    #     cursors = mongo_etl.get_address_in_projects(protocol, ['addresses'])
    #
    #     if cursors:
    #         contract_address = list(cursors['addresses'].keys())
    #         print(f'number of smart contract in {db_prefix} is:  {len(contract_address)}')
    #         # t = threading.Thread(target=process_transaction, args=(db_prefix, contract_address))
    #         # threads.append(t)
    #         # t.start()
    #
    #         t = threading.Thread(target=process_active_user, args=(chain_id, contract_address))
    #         threads.append(t)
    #         t.start()
    #
    # for thread in threads:
    #     thread.join()  # Wait for all threads to finish
    #
    # print('number of contract: ', transaction_count)
    # print('number of active user: ', active_user)

# 490271  total user  aave_holders, aave_users
'''
get 33398 transaction count  in avalanche
get 28128 transaction count  in optimism--
get 61605 transaction count  in arbitrum --
get 46214 transaction count  in ethereum --- 
get 262 transaction count  in ftm --- 
get 138334 contract count  in polygon ---


get 28024 active user count in ethereum---
get 178 active user count in ftm ---
get 19188 active user count in avalanche ---
get 19155 active user count in optimism ---
get 25031 active user count in arbitrum ---
get 71065 active user count in polygon ,--- 142655
'''
