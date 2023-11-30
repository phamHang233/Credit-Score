from databases.lending_mongodv_klg import MongoDB as lending_klg_db
from databases.cs_mongodb_klg import MongoDB as KLG

import json
import click

# from constants.network_constants import Chains
from jobs.statistics.wallet_in_pool_scores_job import WalletInPoolScoreJob
from utils.logger_utils import get_logger

logger = get_logger('Get scores')


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-nc", "--n-cpu", default=1, show_default=True, type=int, help="Number of CPU"
    )
@click.option("-c", "--cpu", default=1, show_default=True, type=int, help="CPU order")
@click.option(
    "-bs",
    "--wallet-batch-size",
    default=25000,
    show_default=True,
    type=int,
    help="Wallet batch size",
    )
@click.option(
    "-w",
    "--max-workers",
    default=12,
    show_default=True,
    type=int,
    help="The number of workers",
    )

def statistic_scores(n_cpu, cpu, wallet_batch_size, max_workers):
    lending_protocols = [ "compound", "venus", "aave", "justlend", "spark", 'morpho', "radiant-v2"]

    """Statistic score from knowledge graph"""
    lending_mongo_klg = lending_klg_db("")
    logger.info(f"Connect to graph: {lending_mongo_klg.connection_url}")

    flagged_state = lending_mongo_klg.get_wallet_flagged_state()
    wallets_batch = flagged_state["batch_idx"]
    logger.info(f"There are {wallets_batch} batches")

    job = WalletInPoolScoreJob(
        batch_size=1,
        max_workers=max_workers,
        db=lending_mongo_klg,
        wallets_batch=wallets_batch,
        n_cpu=n_cpu,
        cpu=cpu,
        wallet_batch_size=wallet_batch_size,
        )

    try:
        logger.info(f"Running Job")
        job.run()

    except KeyboardInterrupt:
        logger.warning("Exit")


    finally:
        deposit_wallet={}
        borrow_wallets_in_pool={}
        for pool in lending_protocols:
            print(f"number of wallet in {pool}: ",len(job.wallets_info_in_pool[pool]))
            deposit_wallet[pool]= list(set(job.deposit_wallets_in_pool[pool]))
            borrow_wallets_in_pool[pool]= list(set(job.borrow_wallets_in_pool[pool]))
            print(f"number borrow wallet in {pool}: ", len(borrow_wallets_in_pool[pool]))
            print(f"number deposit wallet in {pool}: ", len(deposit_wallet[pool]))

        # distribute_health_factor(job.health_factor)
        with open("deposit_and_borrow_wallet.json", "w") as f:
            json.dump(job.wallets_info_in_pool, f, indent=1)
        get_score_of_wallet(deposit_wallet, "deposit")
        get_score_of_wallet(borrow_wallets_in_pool, "borrow")



#
# def distribute_health_factor(data):
#
#     res = {pool:{} for pool in lending_protocols }
#
#     for pool in lending_protocols:
#         number_liquidated_wallet=0
#         number_safe_wallet=0
#         number_dangerous_wallet= 0
#         health_factor_in_pool= data[pool]
#         print(f"health factor in {pool}: {len(health_factor_in_pool)}")
#         for wallet in health_factor_in_pool:
#             hf = health_factor_in_pool[wallet]
#             if hf<1:
#                 number_liquidated_wallet +=1
#             elif hf>1.1:
#                 number_safe_wallet+=1
#             else:
#                 number_dangerous_wallet+=1
#         res[pool] = {"liquidated wallet":number_liquidated_wallet,
#                         "dangerous_wallet":number_dangerous_wallet ,
#                         "safe_wallet":number_safe_wallet }
#     with open('json/health_fator_of_borrow_wallet.json', 'w') as f:
#         json.dump(res, f)


# def get_score_of_wallet()

def get_score_of_wallet(wallets_in_pool, lending_type):

    """
    Poor: 300 - 579
    Fair: 580 - 669
    Good: 670 - 739
    Very Good: 740 - 799
    Exceptional: 800 - 850
    """
    lending_protocols = ["venus", "aave", "justlend", "compound", "spark", 'morpho', "radiant-v2"]

    number_of_wallet = {pool: {} for pool in lending_protocols}
    # res = {pool: {} for pool in lending_protocols}

    for pool in lending_protocols:
        addresses= wallets_in_pool[pool]

        logger.info(f'There are {len(addresses)} wallets')
        db = KLG()

        cursor = db.get_multichain_wallets_scores_by_keys(keys=addresses, projection=['creditScore', 'address'])
        scores = {w['address']: w['creditScore'] for w in cursor}

        levels = ['Poor', 'Fair', 'Good', 'Very Good', 'Exceptional']
        ranges = [300, 580, 670, 740, 800, 850]
        # data = {level: {} for level in levels}
        wallet_by_levels= {level:0 for level in levels}
        for address in addresses:
            score = scores.get(address, 300)
            level = get_level(score, ranges, levels)
            # data[level][address] = score
            wallet_by_levels[level] += 1
            # res[pool][level].append(address)

        number_of_wallet[pool]= wallet_by_levels

    with open(f'json/{lending_type}_wallet.json', 'w') as f:
        json.dump(number_of_wallet, f)
    return number_of_wallet

def get_score_of_all_address(addresses):
    db = KLG()

    cursor = db.get_multichain_wallets_scores_by_keys(keys=addresses, projection=['creditScore', 'address'])
    scores = {w['address']: w['creditScore'] for w in cursor}
    return scores

def find_score_level_of_address(scores, address):
    ranges = [300, 580, 670, 740, 800, 850]
    levels = ['Poor', 'Fair', 'Good', 'Very Good', 'Exceptional']
    score = scores.get(address, 300)
    level = get_level(score, ranges, levels)
    return level


def get_level(score, ranges, levels):
    for idx, r in enumerate(ranges[:-1]):
        e = ranges[idx + 1]
        if r <= score < e:
            return levels[idx]

def get_health_factor_and_level_of_wallet():
    lending_protocols = ["compound", "venus", "aave", "spark", "radiant-v2"]
    bad_debt_wallet_addr = {}
    error_wallet = {}

    with open("deposit_and_borrow_wallet.json", "r") as f:
        data = json.load(f)
    # unique_addresses = set()
    # for _, addr_info in data.items():
    #     for address in addr_info.keys():
    #         unique_addresses.add(address)
    # scores= get_score_of_all_address(list(unique_addresses))
    # with open("json/wallet_scores.json", "w") as f:
    #     json.dump(scores,f)

    with open("json/wallet_scores.json", "r") as f:
        scores= json.load(f)
    # health_factor={}
    percentage_of_debt_wallets = {}

    for pool in lending_protocols:
        bad_debt_wallet_addr[pool]=[]
        error_wallet[pool]= []
        deposit_wallets= []
        borrow_wallets= []
        bad_health_factor=0
        total_borrow= 0
        bad_debt_wallet=0
        total_bad_debt= 0
        total_debt_with_deposit= 0
        for wallet in data[pool]:
            borrow_amount= data[pool][wallet]["borrow_amount"]
            deposit_with_lth= data[pool][wallet]["deposit_with_lth"]

            if deposit_with_lth>0:
                deposit_wallets.append(wallet)
            if borrow_amount>0:
                if deposit_with_lth==0:
                    error_wallet[pool].append(wallet)
                total_borrow+= borrow_amount
                borrow_wallets.append(wallet)
                hf= deposit_with_lth/borrow_amount
                if hf<1:
                    # health_factor[pool]["liquidated_wallet"].append(wallet)
                    bad_health_factor+=1
                    level_wallet= find_score_level_of_address(scores,wallet)
                    if level_wallet== 'Poor' or level_wallet=="Fair":
                        bad_debt_wallet_addr[pool].append(wallet)
                        total_bad_debt+=borrow_amount
                        bad_debt_wallet+=1
                        total_debt_with_deposit += borrow_amount- deposit_with_lth
                # else:
                #     health_factor[pool]["unliquidated_wallet"].append(wallet)

        percentage_of_number_of_bad_debt=  bad_debt_wallet/ len(set(borrow_wallets))
        percentage_of_total_bad_debt_amount= total_bad_debt/total_borrow
        percentage_of_total_bad_debt_with_deposit= total_debt_with_deposit/total_borrow
        percentage_of_debt_wallets[pool] = {
            "bad_healt_factor": bad_health_factor,
            "number_of_bad_debt": bad_debt_wallet,
            "borrow_wallet": len(borrow_wallets),
            "deposit_wallet": len(deposit_wallets),
            "unique_deposit_wallet": len(set(deposit_wallets)),
            "percentage_of_number_of_bad_debt":percentage_of_number_of_bad_debt,
            "percentage_of_total_bad_debt_amount": percentage_of_total_bad_debt_amount,
            "percentage_of_total_bad_debt_with_deposit":percentage_of_total_bad_debt_with_deposit}
    with open("bad_debt_wallet.json", "w") as f:
        json.dump(percentage_of_debt_wallets, f, indent=1)
    with open("json/list_bad_def_wallet.json", "w") as f:
        json.dump(bad_debt_wallet_addr, f, indent=1)

    with open("json/list_error_wallet.json", "w") as f:
        json.dump(error_wallet, f, indent=1)

if __name__ == "__main__":
    # statistic_scores()
    get_health_factor_and_level_of_wallet()
