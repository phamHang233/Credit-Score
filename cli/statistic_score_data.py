import csv
import json

import click
from databases.cs_mongodb_klg import MongoDB as KLG
from jobs.statistics.wallet_score_job import WalletScoresJob
from utils.logger_utils import get_logger

logger = get_logger("Statistic scores")


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-nc", "--n-cpu", default=1, show_default=True, type=int, help="Number of CPU"
)
@click.option("-c", "--cpu", default=1, show_default=True, type=int, help="CPU order")
@click.option(
    "-bs",
    "--wallet-batch-size",
    default=10000,
    show_default=True,
    type=int,
    help="Wallet batch size",
)
@click.option(
    "-w",
    "--max-workers",
    default=10,
    show_default=True,
    type=int,
    help="The number of workers",
)
def statistic_scores(n_cpu, cpu, wallet_batch_size, max_workers):
    """Statistic score from knowledge graph"""
    klg_db = KLG()
    logger.info(f"Connect to graph: {klg_db.connection_url}")

    flagged_state = klg_db.get_wallet_flagged_state()
    wallets_batch = flagged_state["batch_idx"]
    logger.info(f"There are {wallets_batch} batches")

    job = WalletScoresJob(
        batch_size=1,
        max_workers=max_workers,
        db=klg_db,
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
        lending_tokens = job.token_data
        number_wallet = job.n_wallets
        print(f'Number of wallets: {number_wallet}')
        all_tokens = set()

        for level, token_info in lending_tokens.items():
            for token_address in token_info:
                all_tokens.add(token_address)
        levels = ['Poor', 'Fair', 'Good', 'Very Good', 'Exceptional']
        top_token_amount = {level: {} for level in levels}
        top_borrow_token_amount = {level: {} for level in levels}

        cursor = klg_db.get_tokens_by_keys(list(all_tokens))
        tokens_price = {t['_id']: {'price': t.get('price', None), 'idCoingecko': t['idCoingecko']}for t in cursor}

        for level, info in lending_tokens.items():
            for token_address, balance in info.items():
                amount = balance['amount']
                borrow_amount = balance['borrow_amount']
                token_info = tokens_price.get(token_address, {})

                if token_info.get('price'):
                    if amount or borrow_amount:
                        price = token_info['price']
                        token_name = token_info['idCoingecko']
                        amount_in_usd = price * amount
                        borrow_amount_in_usd = price * borrow_amount

                        if token_name not in top_token_amount[level]:
                            top_token_amount[level][token_name] = 0
                        if token_name not in top_borrow_token_amount[level]:
                            top_borrow_token_amount[level][token_name] = 0

                        top_token_amount[level][token_name] += amount_in_usd
                        top_borrow_token_amount[level][token_name] += borrow_amount_in_usd

            top_token_amount[level] = dict(
                sorted(top_token_amount.get(level, {}).items(), key=lambda x: x[1], reverse=True))
            top_borrow_token_amount[level] = dict(
                sorted(top_borrow_token_amount.get(level, {}).items(), key=lambda x: x[1], reverse=True))

        for level, tokens in top_token_amount.items():
            print(f'{level}: {list(tokens.items())[:5]}')
        print("__________")
        for level, tokens in top_borrow_token_amount.items():
            print(f'{level}: {list(tokens.items())[:5]}')

        with open('top_borrow_tokens.json', 'w') as f:
            json.dump(top_borrow_token_amount, f)

        with open('top_tokens.json', 'w') as f:
            json.dump(top_token_amount, f)
    logger.info("Done")


