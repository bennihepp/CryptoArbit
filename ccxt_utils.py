import time
import ccxt

def get_acc_asks_bids(ob):
    acc_asks = []
    acc_volume = 0
    for price, volume in ob['asks']:
        acc_volume += volume
        acc_asks.append((price, acc_volume))
    acc_bids = []
    acc_volume = 0
    for price, volume in ob['bids']:
        acc_volume += volume
        acc_bids.append((price, acc_volume))
    return acc_asks, acc_bids


def get_conservative_ask_bid_price(ob, min_volume):
    acc_asks, acc_bids = get_acc_asks_bids(ob)
    ask_price = float("inf")
    ask_volume = 0.0
    for price, acc_volume in acc_asks:
        if acc_volume >= min_volume:
            ask_price = price
            ask_volume = acc_volume
            break
    bid_price = 0
    bid_volume = 0.0
    for price, acc_volume in acc_bids:
        if acc_volume >= min_volume:
            bid_price = price
            bid_volume = acc_volume
            break
    return ask_price, ask_volume, bid_price, bid_volume


def retry(request_fn, *args, **kwargs):
    if "_max_trials" in kwargs:
        max_trials = kwargs["_max_trials"]
        del kwargs["_max_trials"]
    else:
        max_trials = None
    if "_max_time" in kwargs:
        max_time = kwargs["_max_time"]
        del kwargs["_max_time"]
    else:
        max_time = None
    if "_rate_limit" in kwargs:
        rate_limit = kwargs["_rate_limit"]
        del kwargs["_rate_limit"]
    else:
        rate_limit = 0
    if "_retry_exception_types" in kwargs:
        retry_exception_types = kwargs["_retry_exception_types"]
        del kwargs["_retry_exception_types"]
    else:
        retry_exception_types = ccxt.BaseError
    num_trials = 0
    last_trial_time = -float("inf")
    while True:
        now = time.time()
        if now < last_trial_time + rate_limit:
            time.sleep(last_trial_time + rate_limit - now)
        try:
            num_trials += 1
            if max_time is not None and time.time() > max_time:
                return None
            if max_trials is not None and num_trials > max_trials:
                return None
            last_trial_time = time.time()
            result = request_fn(*args, **kwargs)
        except retry_exception_types as err:
            print("Error on ccxt request: {}. Trying again.".format(err))
            continue
        return result
