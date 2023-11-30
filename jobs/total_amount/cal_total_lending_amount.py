import json
from databases.cs_mongodb_klg import MongoDB as KLG

# from databases.cs_mongodb_klg import MongoDB


def calculate_total_lending_amount():
    with open("apy.json", "r") as f:
        data = json.load(f)
    lending_protocols_data = {}
    prices = {}
    token_names = {}
    collateral_assets_amount = {}
    lending_assets_amount = {}
    collateral_name_assets = {}
    lending_name_assets = {}
    total_borrow = {}
    total_deposit = {}
    capital_efficiency = {}
    # mongo_klg = MongoDB(connection_url="", db_prefix=chain)
    mongo_klg = KLG("")
    for i in data:
        lending_info_with_chain = i[0]
        entity_id = lending_info_with_chain["entity_id"]

        chain_id = lending_info_with_chain["query_id"]
        if entity_id == "aave-v2" or entity_id == "aave-v3":
            unique_pool = "aave"
        elif entity_id == "morpho-aave" or entity_id == "morpho-compound":
            unique_pool = "morpho"
        elif entity_id == "compound-v3" or entity_id == "compound":
            unique_pool = "compound"
        else:
            unique_pool = entity_id

        if unique_pool not in collateral_assets_amount:
            collateral_assets_amount[unique_pool] = {}
            total_borrow[unique_pool] = 0.0
            total_deposit[unique_pool] = 0.0
            lending_assets_amount[unique_pool] = {}
            lending_name_assets[unique_pool] = {}
            collateral_name_assets[unique_pool] = {}

        for _, token_info in lending_info_with_chain["protocol_apy"].items():

            for token, total_amount in token_info.items():
                chain_id_token = f'{chain_id}_{token}'

                if chain_id_token not in prices:
                    try:
                        token_data = mongo_klg.get_smart_contract(chain_id, token)
                        if "name" in token_data:
                            token_name = token_data["name"]
                            token_names[chain_id_token] = token_name

                        price = token_data["price"]
                        prices[chain_id_token] = price
                    except Exception as e:
                        raise e

                else:
                    price = prices[chain_id_token]
                deposit_token_amount_in_usd = total_amount["total_deposit"] * price
                borrow_token_amount_in_usd = total_amount["total_borrow"] * price

                if deposit_token_amount_in_usd > 0:
                    if chain_id_token not in collateral_assets_amount[unique_pool]:
                        collateral_assets_amount[unique_pool][chain_id_token] = deposit_token_amount_in_usd
                    else:
                        collateral_assets_amount[unique_pool][chain_id_token] += deposit_token_amount_in_usd

                    total_deposit[unique_pool] += deposit_token_amount_in_usd
                    # if token_names[token] not in collateral_assets[entity_id]:
                    #     collateral_assets[entity_id][token_names[token]]= deposit_token_amount_in_usd
                    # else:
                    #     collateral_assets[entity_id][token_names[token]]+= deposit_token_amount_in_usd

                if borrow_token_amount_in_usd > 0:
                    if chain_id_token not in lending_assets_amount[unique_pool]:
                        lending_assets_amount[unique_pool][chain_id_token] = borrow_token_amount_in_usd
                    else:
                        lending_assets_amount[unique_pool][chain_id_token] += borrow_token_amount_in_usd

                    total_borrow[unique_pool] += borrow_token_amount_in_usd
                    # if token_names[token] not in lending_assets[entity_id]:
                    #     lending_assets[entity_id][token_names[token]]= borrow_token_amount_in_usd
                    # else:
                    #     lending_assets[entity_id][token_names[token]]+= borrow_token_amount_in_usd

    for unique_pool, borrow_amount in total_borrow.items():
        deposit_amount = total_deposit[unique_pool]
        capital_efficiency[unique_pool] = borrow_amount / deposit_amount * 100

    for pool, tokens_info in lending_assets_amount.items():
        for token_addr, token_amount in tokens_info.items():
            token_name= token_names[token_addr]
            if token_name in lending_name_assets[pool]:
                print(f"token address: {token_addr}\ttoken name:{token_name}" )
                lending_name_assets[pool][token_name] += token_amount

            else:
                lending_name_assets[pool][token_name] = token_amount

    for pool, tokens_info in collateral_assets_amount.items():
        for token_addr, token_amount in tokens_info.items():
            token_name= token_names[token_addr]
            if token_name in collateral_name_assets[pool]:
                print(f"token address: {token_addr}\ttoken name:{token_name}" )
                collateral_name_assets[pool][token_name]+= token_amount
            else:
                collateral_name_assets[pool][token_name]= token_amount


    # group_lending_assets = group_assets(lending_assets)
    # group_deposit_assets = group_assets(collateral_assets)
    res = {
        "collateral_assets_amount: ": collateral_assets_amount,
        "lending_assets_amount:": lending_assets_amount,
        "lending_assets": lending_name_assets,
        "deposit_assets": collateral_name_assets,
        "market_size:": total_deposit,
        "total borrow:": total_borrow,
        "capital_efficiency": capital_efficiency
        }
    with open("lending_protocols_compare.json", "w") as f:
        json.dump(res, f, indent=1)


# def group_assets(assets_with_pool):
#     lending_protocols = ["venus", "aave", "justlend", "compound", "spark", 'morpho', "radiant-v2"]
#     group_assets = {pool: {} for pool in lending_protocols}
#
#     for pool, assets in assets_with_pool.items():
#         if pool == "aave-v2" or pool == "aave-v3":
#             unique_pool = "aave"
#         elif pool == "morpho-aave" or pool == "morpho-compound":
#             unique_pool = "morpho"
#         elif pool == "compound-v3" or pool == "compound":
#             unique_pool = "compound"
#         else:
#             unique_pool = pool
#         group_assets[unique_pool].update(assets)
#     # for pool in lending_protocols:
#     #     group_assets[pool] = list(set(group_assets[pool]))
#
#     return group_assets


if __name__ == "__main__":
    calculate_total_lending_amount()


