import json
import time

from multithread_processing.base_job import BaseJob
from databases.lending_mongodv_klg import MongoDB
from databases.cs_mongodb_klg import MongoDB as KLG

from constants.time_constants import TimeConstants
from databases.smart_contract_label import LabelMongoDb
from utils.list_dict_utils import sort_log, coordinate_logs, get_value_with_default
from utils.logger_utils import get_logger

logger = get_logger('Wallets score Job')
class WalletInPoolScoreJob(BaseJob):
	def __init__(self, batch_size, max_workers, db: MongoDB, wallets_batch, n_cpu=1, cpu=1, wallet_batch_size=10000):
		self.n_cpu = n_cpu
		self.cpu = cpu - 1

		self._db = db

		self.number_of_wallets_batch = wallets_batch
		self.wallet_batch_size = wallet_batch_size

		self.ranges = [300, 580, 670, 740, 800, 850]
		self.levels = ['Poor', 'Fair', 'Good', 'Very Good', 'Exceptional']
		self.lending_protocols = ["compound", "venus", "aave", "justlend",  "spark", 'morpho',"radiant-v2"]
		self.mongo_klg = KLG("")
		self.label_mongo = LabelMongoDb("")
		work_iterable = self._get_work_iterable()
		self.pool_addresses = self.get_pool_addresses()
		self.liquidation_threshold = self.get_all_liquidation_threshold()
		super().__init__(work_iterable, batch_size, max_workers)


	def _get_work_iterable(self):
		work_iterable = [idx for idx in range(1, self.number_of_wallets_batch + 1) if
		                 (idx - 1) % self.n_cpu == self.cpu]
		return work_iterable

	#
	def _init_default(self):
		n_wallets = {level: 0 for level in self.levels}
		deposit_wallets_in_pool ={pool:[] for pool in self.lending_protocols}
		borrow_wallets_in_pool= {pool:[] for pool in self.lending_protocols}
		prices = {}
		health_factor= {pool: {} for pool in self.lending_protocols}
		wallets_info_in_pool= {pool:{} for pool in self.lending_protocols}
		return {
			'n_wallets': n_wallets,
			'deposit_wallets_in_pool': deposit_wallets_in_pool,
			'borrow_wallets_in_pool': borrow_wallets_in_pool,
			'prices':prices,
			'health_factor': health_factor,
			"wallets_info_in_pool":wallets_info_in_pool
			}
		# asset_info = {level: {'number_of_wallets': 0, 'total_assets': 0} for level in self.levels}
		# borrow_info = {level: {'number_of_wallets': 0, 'total_borrow': 0} for level in self.levels}
		# loan_ratio_info = {level: {'number_of_wallets': 0, 'total_loan_ratio': 0} for level in self.levels}
		#
		# liquidate_info = {
		# 	level: {
		# 		'number_of_wallets': 0,
		# 		'liquidated_value': 0,
		# 		'number_of_liquidated': 0
		# 		} for level in self.levels
		# 	}
		#
		# token_data = {level: {} for level in self.levels}
		# return {
		# 	'n_wallets': n_wallets,
		# 	'asset_info': asset_info,
		# 	'borrow_info': borrow_info,
		# 	'loan_ratio_info': loan_ratio_info,
		# 	'liquidation_info': liquidate_info,
		# 	'token_data': token_data
		# 	}
	def _start(self):
		data = self._init_default()

		self.n_wallets = data['n_wallets']
		self.deposit_wallets_in_pool = data['deposit_wallets_in_pool']
		self.borrow_wallets_in_pool = data['borrow_wallets_in_pool']
		self.prices = data['prices']
		self.health_factor = data['health_factor']
		self.wallets_info_in_pool= data['wallets_info_in_pool']

		self.start_exec_time = int(time.time())

	def _end(self):
		self.batch_executor.shutdown()
	def find_pool_address(self, search_key):
		for key, value_list in self.pool_addresses.items():
			for sublist in value_list:
				if search_key in sublist:
					found_in = key
					return found_in
		return None

	def get_pool_addresses(self):
		with open("../total_amount/apy.json", "r") as f:
			data = json.load(f)
		lending_pool_address = {}
		for i in data:
			lending_info_with_chain = i[0]
			entity_id = lending_info_with_chain["entity_id"]
			if entity_id == "aave-v2" or entity_id == "aave-v3":
				entity_id = "aave"
			elif entity_id == "morpho-aave" or entity_id == "morpho-compound":
				entity_id = "morpho"
			elif entity_id == "compound-v3" or entity_id == "compound":
				entity_id = "compound"
			else:
				entity_id = entity_id
			if entity_id not in lending_pool_address:
				lending_pool_address[entity_id] = []
			chain_id = lending_info_with_chain["query_id"]
			addr = list(lending_info_with_chain["protocol_apy"].keys())
			addr_with_chain_id = [chain_id + "_" + item for item in addr]
			lending_pool_address[entity_id].append(addr_with_chain_id)
		return lending_pool_address
	def get_token_price(self, chain_id, token_addr, prices):
		if token_addr not in prices:
			try:
				token_data = self.mongo_klg.get_smart_contract(chain_id, token_addr)
				price = token_data["price"]
				prices[token_addr] = price
			except Exception as e:
				raise e
		else:
			price = prices[token_addr]
		return price
	def get_all_liquidation_threshold(self):
		liquidation_threshold={}
		cursor = self.label_mongo.get_all_doc()

		for doc in cursor:
			try:
				reservesList = doc['reservesList']
				fork = doc['forked']
				for addr, token_info in reservesList.items():
					lth= token_info.get('liquidationThreshold', None)
					if lth is not None:

						liquidation_threshold[addr] = lth

			except Exception as e:
				logger.exception(e)
				continue
		return liquidation_threshold


	def _execute_batch(self, wallets_batch_indicates):
		data = self._init_default()
		# n_wallets = data['n_wallets']
		deposit_wallets_in_pool = data['deposit_wallets_in_pool']
		borrow_wallets_in_pool = data['borrow_wallets_in_pool']
		prices = data['prices']
		health_factor = data['health_factor']
		wallets_info_in_pool= data['wallets_info_in_pool']


		for batch_idx in wallets_batch_indicates:
			try:
				start_time = time.time()
				cursor = self._db.get_mulitchain_wallet(
					{'flagged': batch_idx,
					 '$or': [{'borrowInUSD': {'$gt': 0}}, {'depositInUSD': {'$gt': 0}}]},
					projection=['lendings']
					)

				cnt = 0
				for doc in cursor:
					cnt+=1
					if cnt %5000 ==0:
						logger.info(f"Execute {cnt} ({round(100 * cnt / 50000, 2)}%) wallets on batch [{batch_idx}]")
					wallet_address = doc['_id']


					chain_id_pool_info = doc["lendings"]
					for chain_id_pool_address , pool_info  in chain_id_pool_info.items():
						pool = self.find_pool_address(chain_id_pool_address)
						if pool is not None:

							deposit_amount = 0
							borrow_amount = 0
							deposit_with_LTH = 0

							chain_id = chain_id_pool_address.split("_")[0]
							deposit_tokens = pool_info.get('depositTokens', {})
							borrow_tokens = pool_info.get('borrowTokens', {})
							for token_add, token_amount in deposit_tokens.items():
								if token_amount>0:
									token_price= self.get_token_price(chain_id, token_add, prices)
									token_amount_in_USD = token_price*token_amount
									deposit_amount+= token_amount_in_USD

									lth = self.liquidation_threshold[token_add]
									deposit_with_LTH+= lth*token_amount_in_USD


							for token_add, token_amount in borrow_tokens.items():
								if token_amount>0:
									token_price= self.get_token_price(chain_id, token_add, prices)
									token_amount_in_USD = token_price*token_amount
									borrow_amount+= token_amount_in_USD

							if wallet_address not in wallets_info_in_pool[pool] and (borrow_amount>0 or deposit_amount>0):
								wallets_info_in_pool[pool][wallet_address]= {"borrow_amount":0,
																			"deposit_with_lth":0,
									}

							if borrow_amount > 0:
								borrow_wallets_in_pool[pool].append(wallet_address)
								wallets_info_in_pool[pool][wallet_address]['borrow_amount']+= borrow_amount
								hf = deposit_with_LTH / borrow_amount
								health_factor[pool][wallet_address] = hf

							if deposit_amount > 0:
								deposit_wallets_in_pool[pool].append(wallet_address)
								wallets_info_in_pool[pool][wallet_address]['deposit_with_lth']+= deposit_with_LTH



			except Exception as ex:
				logger.exception(ex)
				continue
		self.combined( deposit_wallets_in_pool, borrow_wallets_in_pool, prices, wallets_info_in_pool)



		logger.info(f'[{wallets_batch_indicates}] Executed, took {time.time() - self.start_exec_time, 3}s')

	def combined(self, deposit_wallets_in_pool, borrow_wallets_in_pool, prices,wallets_info_in_pool ):
		for pool in self.lending_protocols:

			self.deposit_wallets_in_pool[pool]+= deposit_wallets_in_pool[pool]
			self.borrow_wallets_in_pool[pool]+= borrow_wallets_in_pool[pool]
			# self.health_factor[pool].update( health_factor[pool])
			for wallet in wallets_info_in_pool[pool]:
				wallet_info_of_self = self.wallets_info_in_pool[pool].get(wallet,{})
				borrow_amount= wallet_info_of_self.get("borrow_amount",0) +  wallets_info_in_pool[pool][wallet]["borrow_amount"]
				deposit_with_lth=  wallet_info_of_self.get("deposit_with_lth",0) +  wallets_info_in_pool[pool][wallet]["deposit_with_lth"]
				self.wallets_info_in_pool[pool][wallet]= {"borrow_amount":borrow_amount,
														"deposit_with_lth":deposit_with_lth}

		self.prices.update(prices)

