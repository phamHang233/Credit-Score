import csv
import json

import click
from databases.cs_mongodb_klg import MongoDB as KLG
from jobs.statistics.wallet_scores_job import WalletScoresJob
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
    """Statistic score from knowledge graph"""
    klg_db = KLG()
    logger.info(f"Connect to graph: {klg_db.connection_url}")

    flagged_state = klg_db.get_wallet_flagged_state()
    wallets_batch = flagged_state["batch_idx"]
    logger.info(f"There are {wallets_batch} batches")

    job = WalletScoresJob (
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
            "n_wallets": job.n_wallets,
            "asset_info": job.asset_info,
            "loan_ratio_info": job.loan_ratio_info,
            "borrow_info": job.borrow_info,
            "liquidate_info": job.liquidate_info,
            "token_data": job.token_data,
        }
        print("Get data")
        with open("test/credit_score_stats/data.json", "w") as f:
            json.dump(data, f)
            print("write data.json sucessful")

        with open("test/credit_score_stats/levels.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Level",
                    "Number of wallets",
                    "Avg assets",
                    "Avg loan ratio",
                    "Liquidated wallets",
                ]
            )
            for level in job.levels:
                avg_assets = (
                    job.asset_info[level]["total_assets"]
                    / job.asset_info[level]["number_of_wallets"]
                    if job.asset_info[level]["number_of_wallets"]
                    else 0
                )
                avg_loan_ratio = (
                    job.loan_ratio_info[level]["total_loan_ratio"]
                    / job.loan_ratio_info[level]["number_of_wallets"]
                    if job.loan_ratio_info[level]["number_of_wallets"]
                    else 0
                )
                writer.writerow(
                    [
                        level,
                        job.n_wallets[level],
                        avg_assets,
                        avg_loan_ratio,
                        job.liquidate_info[level]["number_of_wallets"],
                    ]
                )

        export_csv(data)
        logger.info("Saved")

    logger.info("Done")


# def draw_mode_range(type_: str, data):
#     for level, histogram in data.items():
#         plt.figure(figsize=(10, 5))

#         # creating the bar plot
#         plt.bar(
#             list(histogram.keys()), list(histogram.values()), color="maroon", width=0.4
#         )

#         plt.xlabel(type_)
#         plt.ylabel("Count")
#         plt.title(f"{type_.capitalize()} Histogram on level {level}")
#         plt.savefig(f"../../../test/credit_score_stats/{type_}_histogram_{level}.png")


# def get_mode(histogram: dict):
#     if not histogram:
#         return None
#     keys = list(histogram.keys())
#     values = list(histogram.values())

#     mode_value = max(values)
#     mode_index = values.index(mode_value)
#     return keys[mode_index]


def export_csv(data):
    with open("test/credit_score_stats/data.csv", "w") as f:
        n_wallets = data["n_wallets"]
        asset_info = data["asset_info"]
        loan_ratio_info = data["loan_ratio_info"]
        borrow_info = data["borrow_info"]
        liquidate_info = data["liquidate_info"]

        writer = csv.writer(f)
        writer.writerow(
            [
                "Level",
                "Number of wallets",
                "Avg assets",
                "Borrow wallets",
                "Avg Borrow amount",
                "Loan ratio",
                "Liquidated wallets",
                "Total Liquidated",
                "Avg Liquidated value",
                "Avg Number of liquidated"
            ]
        )

        for level in ["Poor", "Fair", "Good", "Very Good", "Exceptional"]:
            avg_assets = (
                asset_info[level]["total_assets"]
                / asset_info[level]["number_of_wallets"]
                if asset_info[level]["number_of_wallets"]
                else 0
            )
            avg_loan_ratio = (
                loan_ratio_info[level]["total_loan_ratio"]
                / loan_ratio_info[level]["number_of_wallets"]
                if loan_ratio_info[level]["number_of_wallets"]
                else 0
            )
            avg_borrow_value = (
                borrow_info[level]["total_borrow"]
                / borrow_info[level]["number_of_wallets"]
                if borrow_info[level]["number_of_wallets"]
                else 0
            )

            avg_liquidated_value = (
                liquidate_info[level]["liquidated_value"]
                / liquidate_info[level]["number_of_wallets"]
                if liquidate_info[level]["number_of_wallets"]
                else 0
            )
            avg_number_of_liquidated = (
                liquidate_info[level]["number_of_liquidated"]
                / liquidate_info[level]["number_of_wallets"]
                if liquidate_info[level]["number_of_wallets"]
                else 0
            )
            writer.writerow(
                [
                    level,
                    n_wallets[level],
                    int(avg_assets),
                    borrow_info[level]["number_of_wallets"],
                    int(avg_borrow_value),
                    str(avg_loan_ratio),
                    liquidate_info[level]["number_of_wallets"],
                    liquidate_info[level]["liquidated_value"],
                    int(avg_liquidated_value),
                    avg_number_of_liquidated,
                ]
            )


def export_token(data):
    db = KLG()
    token_data = data["token_data"]
    for level, info in token_data.items():
        info = dict(sorted(info.items(), key=lambda x: x[1]["increased"], reverse=True))
        token_keys = list(info.keys())
        cursor = db.get_tokens_by_keys(token_keys, projection=["name", "symbol"])
        tokens = {t["_id"]: t.get("symbol", "UNKNOWN") for t in cursor}
        rows = []
        for token_key, token_info in info.items():
            rows.append(
                {
                    "Token": tokens.get(token_key, "UNKNOWN").upper(),
                    "Increased amount": int(
                        max(token_info["end_amount"] - token_info["start_amount"], 0)
                    ),
                    "Increased value": int(token_info["increased"]),
                }
            )

        with open(f"test/credit_score_stats/token_data_{level}.csv", "w") as f:
            writer = csv.DictWriter(
                f, fieldnames=["Token", "Increased amount", "Increased value"]
            )
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    # statistic_scores()
    with open("test/credit_score_stats/data.json") as f:
        data = json.load(f)
        export_csv(data)
        # export_token(data)


