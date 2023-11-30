import time

from multithread_processing.base_job import BaseJob

from constants.time_constants import TimeConstants
from databases.cs_mongodb_klg import MongoDB
from utils.list_dict_utils import sort_log, coordinate_logs, get_value_with_default
from utils.logger_utils import get_logger

logger = get_logger('Wallets  Job')


class BorrowWalletJob(BaseJob):
    def __init__(self, batch_size, max_workers, db: MongoDB, wallets_batch, n_cpu=1, cpu=1, wallet_batch_size=10000):
        self.n_cpu = n_cpu
        self.cpu = cpu - 1

        self._db = db

        self.number_of_wallets_batch = wallets_batch
        self.wallet_batch_size = wallet_batch_size

        self.ranges = [0,10,50,100,500,1000,5000,10000,100000,1000000,10000000]

        work_iterable = self._get_work_iterable()
        super().__init__(work_iterable, batch_size, max_workers)

    def _get_work_iterable(self):
        work_iterable = [idx for idx in range(1, self.number_of_wallets_batch + 1) if
                         (idx - 1) % self.n_cpu == self.cpu]
        return work_iterable
#
    def _init_default(self):
        total_borrow_amount= 0
        number_borrow_wallet= 0
        borrow_info={level:0 for level in self.ranges}
        return {
            'total_borrow_amount':total_borrow_amount,
            'number_borrow_wallet':number_borrow_wallet,
            'borrow_info':borrow_info
            }
    def _start(self):
        data = self._init_default()
        self.total_borrow_amount= data['total_borrow_amount']
        self.number_borrow_wallet= data['number_borrow_wallet']
        self.borrow_info= data['borrow_info']
        self.start_exec_time = int(time.time())



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

    def _execute_batch(self, wallets_batch_indicates):
        data = self._init_default()
        total_borrow_amount = data['total_borrow_amount']
        number_borrow_wallet = data['number_borrow_wallet']
        borrow_info = data['borrow_info']

        for batch_idx in wallets_batch_indicates:
            start_time= time.time()
            cursors= self._db.get_wallets_with_batch_idx(batch_idx=batch_idx,chain_id= '0x38', projection=['borrowInUSD','address'])
            cnt = 0

            for cursor in cursors:
                cnt += 1
                if cnt % 5000 == 0:
                    logger.info(f"Execute {cnt} ({round(100 * cnt / 50000, 2)}%) wallets on batch [{batch_idx}]")

                borrow= cursor.get('borrowInUSD',0)
                wallet = cursor['address']
                if borrow>0:
                    total_borrow_amount+=borrow
                    number_borrow_wallet+=1
                    level= self.get_range_of_borrow_amount(borrow)
                    borrow_info[level]+=1
            print("total_borrow_amount: ", total_borrow_amount)
            print("number_borrow_wallet: ", number_borrow_wallet)
            print("borrow_info: ", borrow_info)
            logger.info(f'Time to execute of batch {batch_idx} is {time.time() - start_time} seconds')

        self.combined(total_borrow_amount,number_borrow_wallet, borrow_info)
        logger.info(f'[{wallets_batch_indicates}] Executed, took {time.time() - self.start_exec_time, 3}s')

    def combined(self,total_borrow_amount,number_borrow_wallet, borrow_info ):
        self.total_borrow_amount+=total_borrow_amount
        self.number_borrow_wallet+=number_borrow_wallet
        for level in self.ranges:
            self.borrow_info[level]+=borrow_info[level]
    def get_range_of_borrow_amount(self,amount):
        for idx, thresold in enumerate(self.ranges):
            if amount < thresold:
                return self.ranges[idx - 1]

        return self.ranges[-1]
