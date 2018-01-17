import time
import requests.exceptions
import urllib3.exceptions
import krakenex


CURRENCY_ASSET_DICT = {
    "EUR": "ZEUR",
    "USD": "ZUSD",
    "ETH": "XETH",
}


def read_keys_from_file(filename):
    with open(filename, "r") as fin:
        passphrase = None
        api_key = fin.readline().strip()
        api_secret = fin.readline().strip()
    return api_key, api_secret, passphrase


def create_client_from_file(filename):
    kraken_client = krakenex.API()
    kraken_client.load_key(filename)
    return kraken_client


def get_account_balance(client, currency):
    assert currency in CURRENCY_ASSET_DICT, "Unknown currency: {}".format(currency)
    try:
        response = client.query_private('Balance')
    except requests.exceptions.HTTPError as exc:
        # print("Error during OHLC query: {}".format(exc))
        return None, [exc]
    if "error" in response:
        error = response["error"]
    else:
        error = []
    if "result" in response:
        asset = CURRENCY_ASSET_DICT[currency]
        balance = float(response["result"][asset])
    else:
        balance = None
    return balance, error


def retry_on_error(request_fn, *args, **kwargs):
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
    success = False
    num_trials = 0
    while True:
        try:
            num_trials += 1
            if max_time is not None and time.time() > max_time:
                return None
            if max_trials is not None and num_trials > max_trials:
                return None
            result = None
            response = request_fn(*args, **kwargs)
            if "error" in response:
                error = response["error"]
            else:
                error = []
            if "result" in response:
                result = response["result"]
        except requests.exceptions.HTTPError as exc:
            # print("HTTP error during Kraken query: {}".format(exc))
            error = [exc]
        except requests.exceptions.RequestException as exc:
            error = [exc]
        except urllib3.exceptions.HTTPError as exc:
            error = [exc]
        except ConnectionResetError as exc:
            error = [exc]
        if len(error) > 0:
            print("Error on Kraken request: {}".format(error))
            print("Trying again")
            continue
        assert len(error) == 0, "Errors during query: {}".format(error)
        return result
