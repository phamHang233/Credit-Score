import json

import click
from databases.cs_mongodb_klg import MongoDB as KLG
from jobs.bnb_lending_tvl_borrow.borrow_wallet_job import BorrowWalletJob

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
    default=50000,
    show_default=True,
    type=int,
    help="Wallet batch size",
    )
@click.option(
    "-w",
    "--max-workers",
    default=20,
    show_default=True,
    type=int,
    help="The number of workers",
    )
def statistic_scores(n_cpu, cpu, wallet_batch_size, max_workers):
    """Statistic score from knowledge graph"""
    klg_db = KLG()
    logger.info(f"Connect to graph: {klg_db.connection_url}")

    flagged_state = klg_db.get_wallet_flagged_state('0xa4b1')
    wallets_batch = flagged_state["batch_idx"]
    logger.info(f"There are {wallets_batch} batches")

    job = BorrowWalletJob(
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
        data = {
            'total_borrow_amount': job.total_borrow_amount,
            'number_borrow_wallet': job.number_borrow_wallet,
            'borrow_info': job.borrow_info,
            'total_deposit_amount': job.total_deposit_amount,
            'number_deposit_wallet': job.number_deposit_wallet,
            'deposit_info': job.deposit_info
        }
        with open("lending_info.json", 'w') as f:
            json.dump(data, f)

if __name__=="__main__":
    statistic_scores()