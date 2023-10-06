import json

# from databases.cs_mongodb_klg import MongoDB as KLG
from databases.mongodb import MongoDB


class BorrowAnalytic:
    def __init__(self, mongo: MongoDB, chain_id):
        self.mongo = mongo
        self.chain_id = chain_id

    def count_number_of_borrow_event(self, start_timestamp, end_timestamp):
        events = self.mongo.get_documents(
            "lending_events",
            {
                "event_type": "BORROW",
                "block_timestamp": {"$gte": start_timestamp, "$lt": end_timestamp},
            },
        )
        borrows = {}
        number_of_borrows = {}
        amount_of_borrows = {}
        prices = {}
        print("get docs")
        for event in events:
            if "amount_in_usd" in event:
                amount_in_usd = event["amount_in_usd"]
                print(amount_in_usd)


if __name__ == "__main__":
    start_time = 1690848000
    end_time = 1693526400
    # with open(f"Score/Very Good.json", "r") as f:
    #     wallets = json.loads(f.read())
    # wallets = None
    mongo = MongoDB(connection_url="", db_prefix="ftm")
    # klg = KLG(connection_url="")
    job = BorrowAnalytic(mongo, "0xa")
    job.count_number_of_borrow_event(start_timestamp=start_time, end_timestamp=end_time)
    # jobs.count_number_of_borrow_user(start_timestamp=start_time, end_timestamp=end_time)
    # jobs.count_number_of_borrow_event(start_timestamp=start_time, end_timestamp=end_time, wallets=wallets)
    # calculate_all_chain("borrows")
    print(".---------------------------------.")
    # jobs.count_number_of_liquidate_event(start_timestamp=start_time, end_timestamp=end_time, wallets=wallets)
    # calculate_all_chain("liquidate")
