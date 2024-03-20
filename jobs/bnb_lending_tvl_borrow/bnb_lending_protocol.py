import json

from databases.smart_contract_label import LabelMongoDb
from databases.cs_mongodb_klg import MongoDB as KLG
contract_label= LabelMongoDb()
klg= KLG()

protocols= {}
total_tvl=0
docs= contract_label.get_all_protocol_by_chain('0xa4b1')
for doc in docs:
    _id= doc["_id"]
    protocol= _id.split("_")[1]
    tvl_by_chain = klg.get_info_of_lending_protocol(protocol, ['tvlByChains'])
    bnb_tvl = tvl_by_chain['tvlByChains']['0xa4b1']
    protocols[protocol]= bnb_tvl
    total_tvl+= bnb_tvl
with open('tvl_of_lending_protocol.json', 'w') as f:
    json.dump(protocols,f, indent=1)

