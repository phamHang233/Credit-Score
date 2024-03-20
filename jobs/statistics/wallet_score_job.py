import time
from multithread_processing.base_job import BaseJob
from databases.cs_mongodb_klg import MongoDB
from utils.logger_utils import get_logger

logger = get_logger('Wallets score Job')


class WalletScoresJob(BaseJob):
    def __init__(self, batch_size, max_workers, db: MongoDB, wallets_batch, n_cpu=1, cpu=1, wallet_batch_size=10000):
        self.n_cpu = n_cpu
        self.cpu = cpu - 1

        self._db = db

        self.number_of_wallets_batch = wallets_batch
        self.wallet_batch_size = wallet_batch_size

        self.ranges = [300, 580, 670, 740, 800, 850]
        self.levels = ['Poor', 'Fair', 'Good', 'Very Good', 'Exceptional']

        work_iterable = self._get_work_iterable()
        super().__init__(work_iterable, batch_size, max_workers)

    def _get_work_iterable(self):
        work_iterable = [idx for idx in range(1, self.number_of_wallets_batch + 1) if
                         (idx - 1) % self.n_cpu == self.cpu]
        return work_iterable

    def _init_default(self):
        n_wallets = {level: 0 for level in self.levels}
        token_data = {level: {} for level in self.levels}
        return {
            'n_wallets': n_wallets,
            'token_data': token_data,
        }

    def _start(self):
        data = self._init_default()

        self.n_wallets = data['n_wallets']
        self.token_data = data['token_data']

        self.start_exec_time = int(time.time())

        print("_start function")

    def _end(self):
        print("_end function")

        self.batch_executor.shutdown()

    def get_level(self, score):
        levels = self.levels

        for idx, r in enumerate(self.ranges[:-1]):
            e = self.ranges[idx + 1]
            if r <= score < e:
                return levels[idx]

        raise ValueError(f'Out of range credit score: {score}')

    def _execute_batch(self, wallets_batch_indicates):
        data = self._init_default()
        n_wallets = data['n_wallets']
        token_data = data['token_data']
        for batch_idx in wallets_batch_indicates:
            try:
                start_time = time.time()
                cursor = self._db.get_multichain_wallets_with_flags(
                    flag_idx=batch_idx, projection=['_id'], batch_size=self.wallet_batch_size
                )
                cnt = 0
                scores = {}
                addresses = []
                for doc in cursor:
                    cnt += 1
                    address = doc['_id']
                    addresses.append(address)
                    if cnt % 5000 == 0:
                        docs = self._db.get_multichain_wallets_scores_by_keys(addresses, projection=['creditScore'])
                        scores.update({d['_id']: d['creditScore'] for d in docs})
                        addresses = []
                logger.info(f'Get all credit score wallets on batch [{batch_idx}]')

                cursor = self._db.get_multichain_wallets_with_flags(
                    flag_idx=batch_idx, batch_size=self.wallet_batch_size
                )
                cnt = 0
                for doc in cursor:
                    cnt += 1
                    if cnt % 5000 == 0:
                        logger.info(f"Execute {cnt} ({round(100 * cnt / 50000, 2)}%) wallets on batch [{batch_idx}]")
                    address = doc['_id']
                    score = scores.get(address)
                    level = self.get_level(score)
                    n_wallets[level] += 1

                    for token_address_with_chain, amount in doc['tokens'].items():
                        if token_address_with_chain not in token_data[level]:
                            token_data[level][token_address_with_chain] = {
                                'borrow_amount': 0,
                                'amount': 0
                            }
                        token_data[level][token_address_with_chain]['amount'] += amount

                    for token_address_with_chain, amount in doc['borrowTokens'].items():
                        if token_address_with_chain not in token_data[level]:
                            token_data[level][token_address_with_chain] = {
                                'borrow_amount': 0,
                                'amount': 0
                            }
                        token_data[level][token_address_with_chain]['borrow_amount'] += amount
                logger.info(f'Time to execute of batch {batch_idx} is {time.time() - start_time} seconds')

            except Exception as ex:
                logger.exception(ex)
                continue

        self.concat_token_data(self.token_data, token_data)
        self.combined(n_wallets)
        logger.info(f'[{wallets_batch_indicates}] Executed, took {time.time() - self.start_exec_time, 3}s')
        print("Finish _execute_batch function")

    def combined(self, n_wallets):
        for level in self.levels:
            try:
                self.n_wallets[level] += n_wallets[level]

            except Exception as ex:
                logger.exception(ex)


    def show(self):
        try:
            print(f'Number of wallets: {self.n_wallets}')
        except Exception as ex:
            logger.exception(ex)

    @staticmethod
    def concat_token_data(token_data, increased):
        for level, token_info in increased.items():
            for token_address, info in token_info.items():
                if token_address not in token_data[level]:
                    token_data[level][token_address] = {
                        'amount': 0,
                        'borrow_amount': 0
                    }
                token_data[level][token_address]['amount'] += info['amount']
                token_data[level][token_address]['borrow_amount'] += info['borrow_amount']
