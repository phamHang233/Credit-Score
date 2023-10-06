import json

from constants.network_constants import Chains
# from databases.mongodb_klg import MongoDB
from utils.logger_utils import get_logger

logger = get_logger('Get scores')


def get_data():
    addresses = set()
    data = {}
    for type_ in ['borrower', 'debtor']:
        for chain_id in Chains.names:
            file_name = f'{type_}_{chain_id}'
            with open(f'users/{file_name}.json') as f:
                addresses_ = json.load(f)

            data[file_name] = addresses_
            addresses.update(addresses_)

    return list(addresses), data


def get_score():
    """
    Poor: 300 - 579
    Fair: 580 - 669
    Good: 670 - 739
    Very Good: 740 - 799
    Exceptional: 800 - 850
    """

    addresses, addresses_by_file = get_data()
    logger.info(f'There are {len(addresses)} wallets')
    db = MongoDB()

    cursor = db.get_multichain_wallets_scores_by_keys(keys=addresses, projection=['creditScore', 'address'])
    scores = {w['address']: w['creditScore'] for w in cursor}

    levels = ['Poor', 'Fair', 'Good', 'Very Good', 'Exceptional']
    ranges = [300, 580, 670, 740, 800, 850]
    data = {level: {} for level in levels}
    data_by_file = {}
    for file_name, addresses_ in addresses_by_file.items():
        score_by_levels = {level: 0 for level in levels}
        for address in addresses_:
            score = scores.get(address, 300)
            level = get_level(score, ranges, levels)

            data[level][address] = score
            score_by_levels[level] += 1

        data_by_file[file_name] = score_by_levels

    data_by_file['total'] = {level: len(info) for level, info in data.items()}
    export_data(data_by_file, data)


def export_data(data_by_file, data):
    for file_name, info in data_by_file.items():
        with open(f'export/{file_name}.json', 'w') as f:
            json.dump(info, f)

    for level, info in data.items():
        info = dict(sorted(info.items(), key=lambda x: x[1], reverse=True))
        with open(f'export/{level}.json', 'w') as f:
            json.dump(info, f)


def get_level(score, ranges, levels):
    for idx, r in enumerate(ranges[:-1]):
        e = ranges[idx + 1]
        if r <= score < e:
            return levels[idx]


if __name__ == '__main__':
    get_score()
