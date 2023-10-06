import os


class MongoConfig:
    HOST = os.getenv("MONGO_HOST")
    PORT = os.getenv("MONGO_PORT")
    USERNAME = os.getenv("MONGO_USERNAME")
    PASSWORD = os.getenv("MONGO_PASSWORD")
    CONNECTION_URL = "mongodb://etlReaderAnalysis:etl_reader_analysis__Gr2rEVBXyPWzIrP@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017/"
    DATABASE = "blockchain_etl"


class MongoDbKLGConfig:
    HOST = "mongodb://klgReaderAnalysis:klgReaderAnalysis_4Lc4kjBs5yykHHbZ@35.198.222.97:27017,34.124.133.164:27017,34.124.205.24:27017/"
    USERNAME = "root"
    PASSWORD = "dev123"
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
