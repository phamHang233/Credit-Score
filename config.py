import os


class MongoConfig:
    HOST = os.getenv("MONGO_HOST")
    PORT = os.getenv("MONGO_PORT")
    USERNAME = os.getenv("MONGO_USERNAME")
    PASSWORD = os.getenv("MONGO_PASSWORD")
    CONNECTION_URL = ""
    DATABASE = "blockchain_etl"


class MongoDbKLGConfig:
    HOST =  os.getenv("MONGODB_CONNECTION_URL")
    USERNAME = "root"
    PASSWORD = 'password'
    # KLG_DATABASE = "klg_database"
    KLG_DATABASE = "knowledge_graph"
    KLG = "knowledge_graph"
    WALLETS = "wallets"
    MULTICHAIN_WALLETS = "multichain_wallets"
    DEPOSITS = "deposits"
    BORROWS = "borrows"
    REPAYS = "repays"
    WITHDRAWS = "withdraws"
    LIQUIDATES = "liquidates"
    SMART_CONTRACTS = "smart_contracts"

class LendingMongoDbKLGConfig:
    HOST= ""
    KLG_DATABASE = "lending_knowledge_graph"
    KLG = "lending_knowledge_graph"
    WALLETS = "wallets"
    MULTICHAIN_WALLETS = "multichain_wallets"
    DEPOSITS = "deposits"
    BORROWS = "borrows"
    REPAYS = "repays"
    WITHDRAWS = "withdraws"
    LIQUIDATES = "liquidates"
    SMART_CONTRACTS = "smart_contracts"
class SmartContractLabel:
    CONNECTION_URL= ""
    DATABASE = "SmartContractLabel"
