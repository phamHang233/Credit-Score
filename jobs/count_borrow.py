import csv
import json
import threading

from databases.cs_mongodb_klg import MongoDB as KLG
from databases.mongodb import MongoDB

chain = {
    "0x1": "ethereum",
    "0x38": "bsc",
    "0xfa": "fantom",
    "0xa": "optimism",
    "0xa4b1": "arbitrum",
    "0xa86a": "avalanche",
    "0x89": "polygon",
}


def calculate_all_chain(file, level=""):
    borrows = {}
    number_of_borrows = {}
    amount_of_borrows = {}
    n_borrows = {}
    for chain_id in ["0x38", "0xfa", "0x1", "0x89", "0xa86a", "0xa4b1", "0xa"]:
        with open(f"./{file}/{level}_{chain_id}.json", "r") as f:
            data = json.loads(f.read())
        for key, value in data["amount"].items():
            if key in borrows:
                borrows[key] += value
            else:
                borrows[key] = value

        for key, value in data["n_events"].items():
            if key in n_borrows:
                n_borrows[key] += value
            else:
                n_borrows[key] = value

        for key, value in data["amount_of_events"].items():
            if key in amount_of_borrows:
                amount_of_borrows[key] += value
            else:
                amount_of_borrows[key] = value

        for key, value in data["number_of_events"].items():
            if key in number_of_borrows:
                number_of_borrows[key] += value
            else:
                number_of_borrows[key] = value
    number_of_wallets = len(amount_of_borrows)
    total_number_of_borrow = sum(number_of_borrows.values())
    total_borrow = sum(amount_of_borrows.values())
    mode_amount, mode_key = BorrowAnalytic.find_mode(borrows)
    _, mode_n_key = BorrowAnalytic.find_mode(n_borrows)
    mode_n_total_borrow = 0
    for key, value in number_of_borrows.items():
        if value in n_borrows[mode_n_key]:
            mode_n_total_borrow += amount_of_borrows[key]
    # with open(f"{file}/all_{level}.json", "w") as f:
    #     data = {
    #         "events": borrows,
    #         "n_events": n_borrows,
    #         "number_of_events": number_of_borrows,
    #         "amount_of_events": amount_of_borrows,
    #     }
    #     json.dump(data, f, indent=1)
    try:
        row = [{
            "Level": level,
            "Number of wallets": number_of_wallets,
            "Average amount of wallets": total_borrow / number_of_wallets,
            "Average amount of events": total_borrow / total_number_of_borrow,
            "Mode amount": mode_key,
            "Percentage mode amount": mode_amount / total_borrow,
            "Average number of wallets": total_number_of_borrow / number_of_wallets,
            "Mode number of borrows": mode_n_key,
            "Percentage mode n borrows": mode_n_total_borrow / total_borrow,
            "number of borrows": total_number_of_borrow,
            "Borrow amount": total_borrow,
            "Percentage mode number borrows": len(n_borrows[mode_n_key]) / number_of_wallets

        }]
        with open(f'{file}/level_info.csv', "a") as f:
            writer = csv.DictWriter(
                f, fieldnames=[
                    "Level", "Number of wallets", "Average amount of wallets", "Average amount of events",
                    "Mode amount", "Percentage mode amount", "Average number of wallets",
                    "Mode number of borrows",
                    "Percentage mode n borrows", "number of borrows", "Borrow amount",
                    "Percentage mode number borrows"
                ]
            )
            if f.tell() == 0:
                writer.writeheader()
            writer.writerows(row)
    except Exception as e:
        raise e


    # print("Number of wallets: ", number_of_wallets)
    # print("Average amount of wallets: ", total_borrow / number_of_wallets)
    # print("Mode amount: ", mode_key)
    # print("Percentage mode amount: ", mode_amount / total_borrow)
    # print("Average number of wallets: ", total_number_of_borrow / number_of_wallets)
    # print("Mode number of borrows: ", mode_n_key)
    # print("Percentage mode n borrows: ", mode_n_total_borrow / total_borrow)
    # print("number of borrows:", total_number_of_borrow)
    # print("Borrow amount: ", total_borrow)
    # print("Percentage mode number borrows:", len(n_borrows[mode_n_key]) / number_of_wallets)


class BorrowAnalytic:
    def __init__(self, mongo: MongoDB, mongo_klg: KLG, chain_id):
        self.mongo = mongo
        self.mongo_klg = mongo_klg
        self.chain_id = chain_id
        self.get_ctokens()

    @staticmethod
    def round_one_digit(n):
        n = int(n)
        return round(n, 1 - len(str(n)))

    @staticmethod
    def find_mode(data):
        mode_amount, mode_key, max_ = 0, 0, 0
        for key, value in data.items():
            if len(value) > max_:
                mode_amount = sum(value)
                mode_key = key
                max_ = len(value)
        return mode_amount, mode_key

    def get_ctokens(self):
        self.ctokens = {}
        for i in [
            "0x38_0xfd36e2c2a6789db23113685031d7f16329158384",
            "0x38_0x589de0f0ccf905477646599bb3e5c622c84cc0ba",
            "0x1_0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b",
        ]:
            if self.chain_id in i:
                address = i.split("_")[1]
                contract = self.mongo_klg.get_smart_contract(self.chain_id, address)
                for key, value in contract["lendingInfo"]["reservesList"].items():
                    self.ctokens[value["vToken"]] = key

    def count_number_of_liquidate_event(self, start_timestamp, end_timestamp, wallets, level=""):
        events = self.mongo.get_documents(
            "lending_events",
            {
                "event_type": "LIQUIDATE",
                "block_timestamp": {"$gte": start_timestamp, "$lt": end_timestamp},
            },
        )
        liquidates = {}
        number_of_liquidates = {}
        amount_of_liquidates = {}
        prices = {}
        for event in events:
            wallet = event["user"]
            if wallets and wallet not in wallets:
                continue
            if "debt_asset" in event:
                token = event["debt_asset"]
            elif event["contract_address"] in self.ctokens:
                token = self.ctokens[event["contract_address"]]
            else:
                continue
            if wallet not in number_of_liquidates:
                number_of_liquidates[wallet] = 0
                amount_of_liquidates[wallet] = 0.0
            if "debt_to_cover_in_usd" in event:
                amount_in_usd = event["debt_to_cover_in_usd"]
            else:
                if token not in prices:
                    token_data = self.mongo_klg.get_smart_contract(self.chain_id, token)
                    price = token_data["price"]
                    prices[token] = price
                else:
                    price = prices[token]
                amount_in_usd = event["debt_to_cover"] * price
            round_amount_in_usd = self.round_one_digit(amount_in_usd)
            if round_amount_in_usd not in liquidates:
                liquidates[round_amount_in_usd] = []
            liquidates[round_amount_in_usd].append(amount_in_usd)
            number_of_liquidates[wallet] += 1
            amount_of_liquidates[wallet] += amount_in_usd
        n_liquidates = {}
        for key, value in number_of_liquidates.items():
            round_number_of_borrows = self.round_one_digit(value)
            if round_number_of_borrows not in n_liquidates:
                n_liquidates[round_number_of_borrows] = []
            n_liquidates[round_number_of_borrows].append(value)

        number_of_wallets = len(amount_of_liquidates)
        total_number_of_liquidate = sum(number_of_liquidates.values())
        total_liquidate = sum(amount_of_liquidates.values())
        mode_amount, mode_key = self.find_mode(liquidates)
        _, mode_n_key = self.find_mode(n_liquidates)
        mode_n_total_liquidate = 0
        for key, value in number_of_liquidates.items():
            if value in n_liquidates[mode_n_key]:
                mode_n_total_liquidate += amount_of_liquidates[key]

        # with open(f"./users/debtor_{self.chain_id}.json", "w") as f:
        #     json.dump(list(amount_of_liquidates.keys()), f, indent=1)
        with open(f"../liquidate/{level}_{self.chain_id}.json", "w") as f:
            data = {
                "amount": liquidates,
                "n_events": n_liquidates,
                "number_of_events": number_of_liquidates,
                "amount_of_events": amount_of_liquidates,
            }
            json.dump(data, f, indent=1)

        row = [{
            "Chain Id": self.chain_id,
            "Number of wallets": number_of_wallets,
            "Average amount of wallets": total_liquidate / number_of_wallets,
            "Average amount of events": total_liquidate / total_number_of_liquidate,
            "Mode amount": mode_key,
            "Percentage mode amount": mode_amount / total_liquidate,
            "Average number of wallets": total_number_of_liquidate / number_of_wallets,
            "Mode number of liquidates": mode_n_key,
            "Percentage mode n liquidates": mode_n_total_liquidate / total_liquidate,
            "number of liquidates": total_number_of_liquidate,
            "liquidate amount": total_liquidate,
            "Percentage mode number liquidates": len(n_liquidates[mode_n_key]) / number_of_wallets

        }]
        with open('liquidate/chain_info.csv', "a") as f:
            writer = csv.DictWriter(
                f, fieldnames=[
                    "Chain Id", "Number of wallets", "Average amount of wallets", "Average amount of events",
                    "Mode amount", "Percentage mode amount", "Average number of wallets",
                    "Mode number of liquidates",
                    "Percentage mode n liquidates", "number of liquidates", "liquidate amount",
                    "Percentage mode number liquidates"
                ]
            )
            if f.tell() == 0:
                writer.writeheader()
            writer.writerows(row)
        print("Saved")
        # print("Number of wallets: ", number_of_wallets)
        # print("Average amount of wallets: ", total_liquidate / number_of_wallets)
        # print("Average amount of events: ", total_liquidate / total_number_of_liquidate)
        # print("Mode amount: ", mode_key)
        # print("Percentage mode amount: ", mode_amount / total_liquidate)
        # print(
        #     "Average number of wallets: ", total_number_of_liquidate / number_of_wallets
        # )
        # print("Mode number of liquidates: ", mode_n_key)
        # print(
        #     "Percentage mode n liquidates: ", mode_n_total_liquidate / total_liquidate
        # )
        # print("Number of liquidates:", total_number_of_liquidate)
        # print("Liquidate amount: ", total_liquidate)

    def count_number_of_borrow_event(self, start_timestamp, end_timestamp, wallets, level=""):
        events = self.mongo.get_documents(
            "lending_events",
            {
                "event_type": "BORROW",
                "block_timestamp": {"$gte": start_timestamp, "$lt": end_timestamp},
            },
        )
        print("Get events!")
        borrows = {}
        number_of_borrows = {}
        amount_of_borrows = {}
        prices = {}
        for event in events:
            wallet = event["wallet"]
            if wallets and wallet not in wallets:
                continue
            if "reserve" in event:
                token = event["reserve"]
            elif event["contract_address"] in self.ctokens:
                token = self.ctokens[event["contract_address"]]
            else:
                continue
            if wallet not in number_of_borrows:
                number_of_borrows[wallet] = 0
                amount_of_borrows[wallet] = 0.0
            if "amount_in_usd" in event:
                amount_in_usd = event["amount_in_usd"]
            else:
                if token not in prices:
                    token_data = self.mongo_klg.get_smart_contract(self.chain_id, token)
                    price = token_data["price"]
                    prices[token] = price
                else:
                    price = prices[token]
                amount_in_usd = event["amount"] * price
            round_amount_in_usd = self.round_one_digit(amount_in_usd)
            if round_amount_in_usd not in borrows:
                borrows[round_amount_in_usd] = []
            borrows[round_amount_in_usd].append(amount_in_usd)
            number_of_borrows[wallet] += 1
            amount_of_borrows[wallet] += amount_in_usd
        n_borrows = {}

        for key, value in number_of_borrows.items():
            round_number_of_borrows = self.round_one_digit(value)
            if round_number_of_borrows not in n_borrows:
                n_borrows[round_number_of_borrows] = []
            n_borrows[round_number_of_borrows].append(value)

        number_of_wallets = len(amount_of_borrows)

        # with open(f"./users/borrower_{self.chain_id}.json", "w") as f:
        #     json.dump(list(number_of_borrows.keys()), f, indent=1)
        total_number_of_borrow = sum(number_of_borrows.values())
        total_borrow = sum(amount_of_borrows.values())
        mode_amount, mode_key = self.find_mode(borrows)
        _, mode_n_key = self.find_mode(n_borrows)
        mode_n_total_borrow = 0
        for key, value in number_of_borrows.items():
            if value in n_borrows[mode_n_key]:
                mode_n_total_borrow += amount_of_borrows[key]
        with open(f"./borrows/{level}_{self.chain_id}.json", "w") as f:
            data = {
                "amount": borrows,
                "n_events": n_borrows,
                "number_of_events": number_of_borrows,
                "amount_of_events": amount_of_borrows,
            }
            json.dump(data, f, indent=1)
        row = [{
            "Chain Id": self.chain_id,
            "Number of wallets": number_of_wallets,
            "Average amount of wallets": total_borrow / number_of_wallets,
            "Average amount of events": total_borrow / total_number_of_borrow,
            "Mode amount": mode_key,
            "Percentage mode amount": mode_amount / total_borrow,
            "Average number of wallets": total_number_of_borrow / number_of_wallets,
            "Mode number of borrows": mode_n_key,
            "Percentage mode n borrows": mode_n_total_borrow / total_borrow,
            "number of borrows": total_number_of_borrow,
            "Borrow amount": total_borrow,
            "Percentage mode number borrows": len(n_borrows[mode_n_key]) / number_of_wallets

        }]
        with open(f'borrows/chain_info.csv', "a") as f:
            writer = csv.DictWriter(
                f, fieldnames=[
                    "Chain Id", "Number of wallets", "Average amount of wallets", "Average amount of events",
                    "Mode amount", "Percentage mode amount", "Average number of wallets",
                    "Mode number of borrows",
                    "Percentage mode n borrows", "number of borrows", "Borrow amount",
                    "Percentage mode number borrows"
                ]
            )
            if f.tell() == 0:
                writer.writeheader()
            writer.writerows(row)


def get_wallet_by_chain(chain, chainId):

    start_time = 1688169600  # 1-7-2023
    end_time = 1696118400  # 1-10-2023
    # with open(f"Score/Very Good.json", "r") as f:
    #     wallets = json.loads(f.read())
    klg = KLG("")

    mongo = MongoDB(connection_url="", db_prefix=chain)
    job = BorrowAnalytic(mongo, klg, chainId)
    job.count_number_of_borrow_event(
        start_timestamp=start_time, end_timestamp=end_time, wallets=[]
    )
    job.count_number_of_liquidate_event(start_timestamp=start_time, end_timestamp=end_time, wallets=[])


def get_wallet_by_level(chain, chainId):
    start_time = 1688169600  # 1-7-2023
    end_time = 1696118400  # 1-10-2023
    klg = KLG("")

    mongo = MongoDB(connection_url="", db_prefix=chain)
    job = BorrowAnalytic(mongo, klg, chainId)
    for level in ['Poor', 'Fair', 'Good', 'Very Good', 'Exceptional']:
        with open(f"export/{level}.json", "r") as f:
            data = json.loads(f.read())
            wallets = data.keys()
        # wallets = None

        # job.count_number_of_borrow_event(
        #     start_timestamp=start_time, end_timestamp=end_time, wallets=wallets, level=level
        # )
        job.count_number_of_liquidate_event(
            start_timestamp=start_time, end_timestamp=end_time, wallets=wallets, level=level
        )
        print(f"Count number of event in {chain} {level} sucessful! ")

if __name__ == "__main__":
    t1 = threading.Thread(target=get_wallet_by_chain,args=("ethereum","0x1",),)
    t2 = threading.Thread(target=get_wallet_by_chain,args=("","0x38",),)
    t3 = threading.Thread(target=get_wallet_by_chain,args=("ftm","0xfa",),)
    t4 = threading.Thread(target=get_wallet_by_chain,args=("optimism","0xa",),)
    t5 = threading.Thread(target=get_wallet_by_chain,args=("arbitrum","0xa4b1",),)
    t6 = threading.Thread(target=get_wallet_by_chain,args=("avalanche","0xa86a",),)
    t7 = threading.Thread(target=get_wallet_by_chain,args=("polygon","0x89",),)

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    # get_wallet_by_level("ftm", "0xfa")
    # get_wallet_by_level("ethereum", "0x1")
    # get_wallet_by_level("polygon", "0x89")
    # get_wallet_by_level("", "0x38")
    # get_wallet_by_level("optimism", "0xa")
    # get_wallet_by_level("arbitrum", "0xa4b1")
    # get_wallet_by_level("avalanche", "0xa86a")
    # for level in ['Poor', 'Fair', 'Good', 'Very Good', 'Exceptional']:
    #
    #     calculate_all_chain("borrows", level=level)


    # start_time = 1688169600  # 1-7-2023
    # end_time = 1696118400  # 1-10-2023
    # # with open(f"Score/Very Good.json", "r") as f:
    # #     wallets = json.loads(f.read())
    # # wallets = None
    # klg = KLG("")
    #
    # mongo = MongoDB(connection_url="", db_prefix="ethereum")
    # job = BorrowAnalytic(mongo, klg, "0x1")
    # job.count_number_of_borrow_event(
    #     start_timestamp=start_time, end_timestamp=end_time, wallets=[]
    # )
    # calculate_all_chain("borrows")
    # job.count_number_of_liquidate_event(start_timestamp=start_time, end_timestamp=end_time, wallets=[])
    # print(".---------------------------------.")

    # calculate_all_chain("liquidate")
