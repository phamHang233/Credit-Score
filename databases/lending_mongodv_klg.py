from pymongo import MongoClient

from config import LendingMongoDbKLGConfig
from utils.logger_utils import get_logger

logger = get_logger("MongoDB")


class MongoDB:
	def __init__(self, graph=None):
		if not graph:
			graph = LendingMongoDbKLGConfig.HOST

		self.connection_url = graph.split("@")[-1]
		self.connection = MongoClient(graph)
		self.mongo_db = self.connection[LendingMongoDbKLGConfig.KLG_DATABASE]

		self._wallets_col = self.mongo_db["wallets"]
		self._multichain_wallets_col = self.mongo_db["multichain_wallets"]
		self._multichain_wallets_credit_scores_col = self.mongo_db[
			"multichain_wallets_credit_scores_v3"

		]
		self._smart_contracts_col = self.mongo_db["smart_contracts"]

		self._profiles_col = self.mongo_db["profiles"]

		self._configs_col = self.mongo_db["configs"]

	def get_mulitchain_wallet(
			self, filter_statement, batch_size=100000, projection=None):
		try:
			cursor = self._multichain_wallets_col.find(filter_statement, projection=projection,
			                                           batch_size=batch_size)
			return cursor
		except Exception as ex:
			logger.exception(ex)
		return None

	def get_projection_statement(projection: list = None):
		if projection is None:
			return None

		projection_statements = {}
		for field in projection:
			projection_statements[field] = True

		return projection_statements


	def get_wallet_flagged_state(self, chain_id=None):
		if chain_id is None:
			key = 'multichain_wallets_flagged_state'
		else:
			key = f'wallets_flagged_state_{chain_id}'
		filter_statement = {
			"_id": key
			}
		config = self._configs_col.find_one(filter_statement)
		if not config:
			return None
		return config
