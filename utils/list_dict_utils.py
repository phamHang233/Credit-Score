import time
from itertools import islice

from constants.time_constants import TimeConstants
from utils.time_utils import round_timestamp


def filter_none_keys(input_dict: dict):
    none_keys = list()
    for key, value in input_dict.items():
        if value is None:
            none_keys.append(key)

    for key in none_keys:
        input_dict.pop(key, '')

    return input_dict


def extend_abi(abi: list, added_abi: list):
    abi_names = {a['name']: 1 for a in abi if a.get('name')}
    for abi_ in added_abi:
        if abi_.get('name') and abi_['name'] in abi_names:
            continue

        abi.append(abi_)
    return abi


def to_change_logs(d: dict):
    return {int(t): v for t, v in d.items()}


def sorted_dict(d: dict, reverse=False):
    return dict(sorted(d.items(), key=lambda x: x[0], reverse=reverse))


def sort_log(log):
    log = to_change_logs(log)
    log = sorted_dict(log)
    return log


def sort_log_dict(log_dict):
    for key in log_dict:
        log = log_dict[key]
        log_dict[key] = sort_log(log)
    return log_dict


def cut_change_logs(change_logs: dict, end_time: int = None, start_time: int = None, duration: int = None, alt_value=None):
    if not end_time:
        end_time = int(time.time())

    if not start_time:
        if not duration:
            raise ValueError('start_time or duration must be set')
        else:
            start_time = end_time - duration

    change_logs = to_change_logs(change_logs)
    change_logs = sorted_dict(change_logs)
    for t in change_logs.keys():
        if (t < start_time) or (t > end_time):
            change_logs[t] = alt_value

    return change_logs


def chunks_dict(data: dict, size=50):
    it = iter(data)
    for _ in range(0, len(data), size):
        yield {k: data[k] for k in islice(it, size)}


def prune_change_logs(value, change_logs=None, duration=TimeConstants.DAYS_31, interval=TimeConstants.A_DAY):
    if change_logs is None:
        change_logs = {}
    else:
        change_logs = sort_log(change_logs)

    current_time = int(time.time())
    out_date_time = current_time - duration

    change_logs[current_time] = value
    batch = {}
    for k in change_logs:
        if k < out_date_time:
            change_logs[k] = None
        else:
            batch_idx = k // interval
            if batch.get(batch_idx):
                change_logs[k] = None
            else:
                batch[batch_idx] = True
    return change_logs


def combined_logs(*logs, handler_func=sum, default_value=0):
    timestamps = set()
    for log in logs:
        timestamps.update(list(log.keys()))
    timestamps = sorted(timestamps)

    combined = {}
    current_values = [default_value] * len(logs)
    for t in timestamps:
        for idx, log in enumerate(logs):
            current_values[idx] = log.get(t, current_values[idx])

        combined[t] = handler_func(current_values)

    return combined


def combined_token_change_logs_func(values):
    value_in_usd = 0
    for value in values:
        if value is not None:
            value_in_usd += value['valueInUSD']
    return value_in_usd


def coordinate_logs(
        change_logs,
        start_time=0, end_time=None, frequency=None,
        fill_start_value=False, default_start_value=0
):
    if end_time is None:
        end_timestamp = int(time.time())
    else:
        end_timestamp = end_time

    logs = {}
    last_timestamp = 0
    pre_value = default_start_value
    for t, v in change_logs.items():
        if t is None:
            continue

        if t < start_time:
            pre_value = v
        elif start_time <= t <= end_timestamp:
            last_timestamp = _filter_timestamp_in_range(logs, t, v, last_timestamp, frequency)
        elif t > end_timestamp:
            break

    logs = _fill_margin(logs, start_time, end_time, fill_start_value, pre_value, end_timestamp)
    return logs


def _filter_timestamp_in_range(logs: dict, t, v, last_timestamp, frequency):
    if frequency:
        if round_timestamp(t, frequency) != round_timestamp(last_timestamp, frequency):
            logs[t] = v
            last_timestamp = t
    else:
        logs[t] = v

    return last_timestamp


def _fill_margin(logs: dict, start_time, end_time, fill_start_value, pre_value, end_timestamp):
    if (start_time not in logs) and fill_start_value and (pre_value is not None):
        logs[start_time] = pre_value

    logs = sort_log(logs)

    last_value = list(logs.values())[-1] if logs else None
    if (end_time is None) and (last_value is not None):
        logs[end_timestamp] = last_value

    return logs


def get_value_with_default(d: dict, key, default=None):
    """
    The get_value_with_default function is a helper function that allows us to
    get the value of a key in a dictionary, but if the key does not exist or the value of the key is None, it will
    return the default value. This is useful for when we want to check whether something
    exists in our data structure without having to explicitly write code that checks
    for its existence. For example:

    Args:
        d:dict: Specify the dictionary to be used
        key: Retrieve the value from a dictionary
        default: Set a default value if the key is not found in the dictionary or the value of the key is None

    Returns:
        The value of the key if it exists in the dictionary or the value of the key is None,
        otherwise it returns default

    Doc Author:
        Trelent
    """
    if not d:
        return default

    v = d.get(key)
    if v is None:
        v = default
    return v
