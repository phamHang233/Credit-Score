import json

from databases.cs_mongodb_klg import MongoDB as KLG

klg = KLG()

res = {}
cnt = 0
exception = {'0x1_0x96543ef8d2c75c26387c1a319ae69c0bee6f3fe7': 'kujira',
             '0xa_0x3a18dcc9745edcd1ef33ecb93b0b6eba5671e7ca': 'kujira',
             '0xa4b1_0x3a18dcc9745edcd1ef33ecb93b0b6eba5671e7ca': 'kujira',
             '0x38_0x073690e6ce25be816e68f32dca3e11067c9fb5cc': 'kujira',
             '0x1_0x1b3c515f58857e141a966b33182f2f3feecc10e9': 'usk',
             '0x1_0x525a8f6f3ba4752868cde25164382bfbae3990e1': 'nym',
             '0x1_0xa670d7237398238de01267472c6f13e5b8010fd1': 'sommelier',
             '0x1_0x70edf1c215d0ce69e7f16fd4e6276ba0d99d4de7': 'cheqd-network'}

cursor = klg.get_multichain_wallets_scores_by_score(820, projection=[], )

addresses = [doc['_id'] for doc in cursor]
cursor = klg.get_multichain_wallets_by_keys(addresses)
print('get wallet')

for doc in cursor:
    cnt += 1
    if cnt % 4000 == 0:
        print(f'get {cnt} wallets info')

    tokens = doc.get('tokens')
    wallet = doc['_id']
    res[wallet] = {}
    if tokens:
        for token, amount in tokens.items():
            if token in exception:
                res[wallet].update({exception[token]: amount})
                if amount > 1e10:
                    print(f'{wallet}: {exception[token]}: {amount}')

with open('check.json', 'w') as f:
    json.dump(res, f)
