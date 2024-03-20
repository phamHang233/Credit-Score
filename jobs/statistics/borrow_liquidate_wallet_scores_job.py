import time

from multithread_processing.base_job import BaseJob

from constants.time_constants import TimeConstants
from databases.cs_mongodb_klg import MongoDB
from utils.list_dict_utils import sort_log, coordinate_logs, get_value_with_default
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
#
    def _init_default(self):
        n_wallets = {level: 0 for level in self.levels}

        asset_info = {level: {'number_of_wallets': 0, 'total_assets': 0} for level in self.levels}
        borrow_info = {level: {'number_of_wallets': 0, 'total_borrow': 0} for level in self.levels}
        loan_ratio_info = {level: {'number_of_wallets': 0, 'total_loan_ratio': 0} for level in self.levels}

        liquidate_info = {
            level: {
                'number_of_wallets': 0,
                'liquidated_value': 0,
                'number_of_liquidated': 0
            } for level in self.levels
        }

        token_data = {level: {} for level in self.levels}
        return {
            'n_wallets': n_wallets,
            'asset_info': asset_info,
            'borrow_info': borrow_info,
            'loan_ratio_info': loan_ratio_info,
            'liquidation_info': liquidate_info,
            'token_data': token_data
        }

    def _start(self):
        data = self._init_default()

        self.n_wallets = data['n_wallets']
        self.asset_info = data['asset_info']
        self.borrow_info = data['borrow_info']
        self.loan_ratio_info = data['loan_ratio_info']
        self.liquidate_info = data['liquidation_info']
        self.token_data = data['token_data']

        self.start_exec_time = int(time.time())

        self.start_time = 1688169600  #1-7-2023
        self.end_time =  1696118400   #1-10-2023
        print("_start function")

    def _end(self):
        # For save
        # with open('test/increased_rank.csv', 'w') as f:
        #     writer = csv.writer(f)
        #     writer.writerow(['Address', 'Old Level', 'New Level', 'Score Changed'])
        #     writer.writerows(self.increased_rank)
        #
        # with open('test/decreased_rank.csv', 'w') as f:
        #     writer = csv.writer(f)
        #     writer.writerow(['Address', 'Old Level', 'New Level', 'Score Changed'])
        #     writer.writerows(self.decreased_rank)
        #
        print("_end function")
        self.batch_executor.shutdown()


    def is_good(self, score):
        return score >= self.ranges[2]

    def is_bad(self, score):
        return score < self.ranges[2]

    def get_level(self, score):
        levels = self.levels

        for idx, r in enumerate(self.ranges[:-1]):
            e = self.ranges[idx + 1]
            if r <= score < e:
                return levels[idx]

        raise ValueError(f'Out of range credit score: {score}')

    def increase_rank(self, credit_score_logs):
        log = sort_log(credit_score_logs)
        log = coordinate_logs(log, start_time=self.start_time, end_time=self.end_time)
        if not log:
            return None, None, None, 0

        values = list(log.values())
        start_v = values[0]
        end_v = values[-1]

        if self.is_bad(start_v) and self.is_good(end_v):
            return True, self.get_level(start_v), self.get_level(end_v), abs(end_v - start_v)

        if self.is_good(start_v) and self.is_bad(end_v):
            return False, self.get_level(start_v), self.get_level(end_v), abs(end_v - start_v)

        return None, None, None, 0

    def _execute_batch(self, wallets_batch_indicates):
        data = self._init_default()
        n_wallets = data['n_wallets']
        asset_info = data['asset_info']
        borrow_info = data['borrow_info']
        loan_ratio_info = data['loan_ratio_info']
        liquidate_info = data['liquidation_info']
        token_data = data['token_data']

        for batch_idx in wallets_batch_indicates:
            try:
                start_time = time.time()
                cursor = self._db.get_multichain_wallets_scores_by_flag(
                    flag_idx=batch_idx,
                    projection=['creditScore']
                )
                scores = {doc['_id']: doc['creditScore'] for doc in cursor}

                cursor = self._db.get_multichain_wallets_with_flags(
                    flag_idx=batch_idx, projection=[
                        'balanceInUSD',
                        'depositInUSD',
                        'borrowInUSD',
                        'tokenChangeLogs',
                        'liquidationLogs'
                    ], batch_size=self.wallet_batch_size
                )
                cnt = 0
                for doc in cursor:
                    cnt += 1
                    if cnt % 5000 == 0:
                        logger.info(f"Execute {cnt} ({round(100 * cnt / 50000, 2)}%) wallets on batch [{batch_idx}]")

                    address = doc['_id']
                    if not scores.get(address):
                        continue

                    level = self.get_level(scores.get(address))
                    n_wallets[level] += 1

                    deposit_in_usd = get_value_with_default(doc, key='depositInUSD', default=0)
                    borrow_in_usd = get_value_with_default(doc, key='borrowInUSD', default=0)
                    balance_in_usd = get_value_with_default(doc, key='balanceInUSD', default=0)

                    total_asset = balance_in_usd + deposit_in_usd - borrow_in_usd
                    if total_asset > 0:
                        if total_asset > 1e11:
                            logger.warning(f'Ignore wallet total assets ${total_asset}')
                        else:
                            asset_info[level]['number_of_wallets'] += 1
                            asset_info[level]['total_assets'] += total_asset

                        if borrow_in_usd > 0:
                            borrow_info[level]['number_of_wallets'] += 1
                            borrow_info[level]['total_borrow'] += borrow_in_usd

                            loan_ratio = min(borrow_in_usd / total_asset, 1e6)
                            loan_ratio_info[level]['number_of_wallets'] += 1
                            loan_ratio_info[level]['total_loan_ratio'] += loan_ratio

                    # Check if having liquidated
                    liquidation_logs = doc.get('liquidationLogs')
                    if liquidation_logs and liquidation_logs.get('liquidatedWallet'):
                        liquidated_value = 0
                        liquidated_number = 0
                        for buyer, liquidation_log in liquidation_logs['liquidatedWallet'].items():
                            if len(buyer) < 30:
                                continue

                            liquidated_log = sort_log(liquidation_log)
                            liquidated_log = coordinate_logs(
                                liquidated_log, start_time=self.start_time, end_time=self.end_time)

                            liquidated_value += sum([v['debtAssetInUSD'] for v in liquidated_log.values()])
                            liquidated_number += len(liquidated_log)

                        if liquidated_number > 0:
                            liquidate_info[level]['number_of_wallets'] += 1
                            liquidate_info[level]['liquidated_value'] += liquidated_value
                            liquidate_info[level]['number_of_liquidated'] += liquidated_number

                    tokens = doc.get('tokenChangeLogs') or {}
                    token_increased = {}
                    for token_address, token_log in tokens.items():
                        token_log = sort_log(token_log)
                        token_log = coordinate_logs(
                            token_log, start_time=self.start_time, end_time=self.end_time,
                            frequency=TimeConstants.A_DAY, fill_start_value=True,
                            default_start_value={'amount': 0, 'valueInUSD': 0}
                        )
                        increased = get_increase(token_log)
                        if increased['increased'] > 0:
                            token_increased[token_address] = increased

                    self.concat_token_data(token_data, {level: token_increased})

                logger.info(f'Time to execute of batch {batch_idx} is {time.time() - start_time} seconds')
            except Exception as ex:
                logger.exception(ex)
                continue

        self.concat_token_data(self.token_data, token_data)
        self.combined(n_wallets, asset_info, borrow_info, loan_ratio_info, liquidate_info)
        logger.info(f'[{wallets_batch_indicates}] Executed, took {time.time() - self.start_exec_time, 3}s')
        print("Finish _execute_batch function")

    def combined(self, n_wallets, asset_info, borrow_info, loan_ratio_info, liquidate_info):
        for level in self.levels:
            try:
                self.n_wallets[level] += n_wallets[level]

                self.asset_info[level]['number_of_wallets'] += asset_info[level]['number_of_wallets']
                self.asset_info[level]['total_assets'] += asset_info[level]['total_assets']

                self.borrow_info[level]['number_of_wallets'] += borrow_info[level]['number_of_wallets']
                self.borrow_info[level]['total_borrow'] += borrow_info[level]['total_borrow']

                self.loan_ratio_info[level]['number_of_wallets'] += loan_ratio_info[level]['number_of_wallets']
                self.loan_ratio_info[level]['total_loan_ratio'] += loan_ratio_info[level]['total_loan_ratio']

                self.liquidate_info[level]['number_of_wallets'] += liquidate_info[level]['number_of_wallets']
                self.liquidate_info[level]['liquidated_value'] += liquidate_info[level]['liquidated_value']
                self.liquidate_info[level]['number_of_liquidated'] += liquidate_info[level]['number_of_liquidated']

                self.token_data[level] = dict(
                    sorted(self.token_data.get(level, {}).items(), key=lambda x: x[1]['increased'], reverse=True))
            except Exception as ex:
                logger.exception(ex)

        self.show()

    def show(self):
        try:
            print(f'Number of wallets: {self.n_wallets}')

            avg_assets = {
                level: divide(self.asset_info[level]['total_assets'], self.asset_info[level]['number_of_wallets'])
                for level in self.levels
            }
            print(f'Avg assets: {avg_assets}')

            total_borrow = {
                level: {
                    'number_of_wallets': self.borrow_info[level]['number_of_wallets'],
                    'total_borrow': self.borrow_info[level]['total_borrow']
                } for level in self.levels
            }
            print(f'Total borrow: {total_borrow}')

            avg_loan_ratio = {
                level: divide(self.loan_ratio_info[level]['total_loan_ratio'],
                              self.loan_ratio_info[level]['number_of_wallets'])
                for level in self.levels
            }
            print(f'Avg loan ratio: {avg_loan_ratio}')

            total_liquidated = {
                level: {
                    'number_of_wallets': self.liquidate_info[level]['number_of_wallets'],
                    'total_liquidated_value': self.liquidate_info[level]['liquidated_value'],
                    'total_number_of_liquidated': self.liquidate_info[level]['number_of_liquidated']
                } for level in self.levels
            }
            print(f'Liquidated: {total_liquidated}')

            print('Token increased:')
            for level, tokens in self.token_data.items():
                print(f'{level}: {list(tokens.items())[:5]}')
        except Exception as ex:
            logger.exception(ex)

    @staticmethod
    def concat_token_data(token_data, increased):
        for level, token_info in increased.items():
            for token_address, info in token_info.items():
                if token_address not in token_data[level]:
                    token_data[level][token_address] = {
                        'start_value': 0,
                        'start_amount': 0,
                        'end_value': 0,
                        'end_amount': 0,
                        'increased': 0
                    }
                token_data[level][token_address]['start_value'] += info['start_value']
                token_data[level][token_address]['start_amount'] += info['start_amount']
                token_data[level][token_address]['end_value'] += info['end_value']
                token_data[level][token_address]['end_amount'] += info['end_amount']
                token_data[level][token_address]['increased'] += info['increased']


def get_increase(log: dict):
    values = list(log.values()) or [{'amount': 0, 'valueInUSD': 0}]
    start_v = values[0]
    end_v = values[-1]

    increased = end_v['valueInUSD'] - start_v['valueInUSD']
    return {
        'start_value': start_v['valueInUSD'],
        'start_amount': start_v['amount'],
        'end_value': end_v['valueInUSD'],
        'end_amount': end_v['amount'],
        'increased': increased
    }


def divide(a, b):
    if not b:
        return None
    else:
        return a / b


def round_one_digit(n):
    n = int(n)
    return round(n, 1 - len(str(n)))


def count_dict(d, k, v=1):
    if k not in d:
        d[k] = 0
    d[k] += v
