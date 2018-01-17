import sys
import time
import datetime
import math
import random
import uuid
import logging
import numpy as np

import ccxt
import ccxt_utils

import gdax_wrapper
# import gdax_accounts
import kraken_wrapper
# import kraken_accounts


api_rate_limit = 1.0

crypto = "ETH"
fiat = "EUR"
symbol = "{}/{}".format(crypto, fiat)

def ccxt_retry(*args, **kwargs):
    if "_rate_limit" not in kwargs:
        kwargs["_rate_limit"] = api_rate_limit
    return ccxt_utils.retry(*args, **kwargs)


gdax_api_key, gdax_api_secret, gdax_password = gdax_wrapper.read_keys_from_file("gdax_private.key")
gdax = ccxt.gdax()
gdax.apiKey = gdax_api_key
gdax.secret = gdax_api_secret
gdax.password = gdax_password
ccxt_retry(gdax.loadMarkets, reload=True)
gdax_fee = max(gdax.market(symbol)["maker"], gdax.market(symbol)["taker"])

kraken_api_key, kraken_api_secret, kraken_password = kraken_wrapper.read_keys_from_file("kraken_private.key")
kraken = ccxt.kraken()
kraken.apiKey = kraken_api_key
kraken.secret = kraken_api_secret
kraken.password = kraken_password
ccxt_retry(kraken.loadMarkets, reload=True)
kraken_fee = max(kraken.market(symbol)["maker"], kraken.market(symbol)["taker"])
