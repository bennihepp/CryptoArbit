import sys
import os
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
import kraken_wrapper

home_folder = os.environ["HOME"]
logging.basicConfig(filename=os.path.join(home_folder, 'ccxt_arbitration_new.log'), level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

BUY_KRAKEN_SELL_GDAX = 0
BUY_GDAX_SELL_KRAKEN = 1
ARBITRATION_MODES = [BUY_KRAKEN_SELL_GDAX, BUY_GDAX_SELL_KRAKEN]
ARBITRATION_MODES_STR = {
    BUY_KRAKEN_SELL_GDAX: "BUY_KRAKEN_SELL_GDAX",
    BUY_GDAX_SELL_KRAKEN: "BUY_GDAX_SELL_KRAKEN",
}

random.seed()

api_rate_limit = 1.0
balance_update_interval = 10

crypto = "ETH"
fiat = "EUR"
symbol = "{}/{}".format(crypto, fiat)
num_iterations = sys.maxsize
max_num_arbitrations = sys.maxsize
# max_num_arbitrations = 1

# Volume and performance bounds
min_volume_crypto = 0.0
max_volume_crypto = 0.5
# min_gain_percentage = 0.2
# high_gain_percentage = 0.6
# high_gain_reserve = 0.25
# min_relative_gain = min_gain_percentage / 100.0
# high_relative_gain = high_gain_percentage / 100.0
min_gains_percentage = {
    BUY_KRAKEN_SELL_GDAX: [2.0, 1.5, 1.0, 0.75],
    # BUY_GDAX_SELL_KRAKEN: [0.0, -0.5, -1.0],
    # BUY_GDAX_SELL_KRAKEN: [0.4, 0.2, 0.0, -0.2],
    BUY_GDAX_SELL_KRAKEN: [0.2, -0.2, -0.2, -0.2],
}
min_fiat_reserves = {
    BUY_KRAKEN_SELL_GDAX: [0.0, 0.4, 0.6, 0.75],
    BUY_GDAX_SELL_KRAKEN: [0.0, 0.25, 0.4, 0.6],
}
# min_fiat_reserves = {
#     BUY_KRAKEN_SELL_GDAX: [0.0, 2500, 5000, 7500],
#     BUY_GDAX_SELL_KRAKEN: [0.0, 2500, 5000, 7500],
# }
min_relative_gains = {}
for key, gains in min_gains_percentage.items():
    min_relative_gains[key] = [gain / 100.0 for gain in gains]

# Kraken request settings
kraken_timeout = 10.0
kraken_order_book_timeout = 15.0
kraken_add_order_timeout = 10.0
kraken_ohlc_interval = 5  # Interval in minutes
kraken_order_book_count = 100  # Maximum number of active orders to return
# Price rounding (also used for Gdax)
fiat_ndigits = 2
crypto_ndigits = 4

# Simulate arbitration or prompt user input?
simulate = False
# simulate = True
prompt_user = False
# prompt_user = True

# Sleep times
trial_sleep_time = 1
order_check_interval = 2
check_order_time = 15

# Use USD on Kraken?
use_kraken_usd = False

# Fee parameters
gdax_fee_ratio = 0.3 / 100.0
# # kraken_fee_ratio = 0.26 / 100.0
# # kraken_fee_ratio = 0.24 / 100.0
# kraken_fee_ratio = 0.22 / 100.0
# # kraken_fee_ratio = 0.0

# Safety parameters
limit_price_safety_factor = 1.05
buy_safety_factor_fiat = 1.25
min_volume_factor = 10.0
max_time_from_order_book_to_order = 15  # Maximum time allowed between order book results and ordering
max_balance_deviation_crypto = 1e-2
safety_lower_gain_tolerance = 0.8
max_overall_fiat_loss = 25.0


# # Test settings
# min_volume_crypto = 0.02
# max_volume_crypto = 0.02
# min_gain_percentage = 2.0
# min_relative_gain = min_gain_percentage / 100.0

# TODO
kraken_client = kraken_wrapper.create_client_from_file("kraken_private.key")
kraken_pair = "XETHZEUR"
kraken_fiat_currency = "EUR"


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


def prompt_yes_no(message):
    response = input("{} [yes,no] ".format(message))
    if response == "yes":
        return True
    return False

if len(sys.argv) > 1:
    input_order_volume_crypto = float(sys.argv[1])
else:
    input_order_volume_crypto = None


total_balance_fiat_begin = None

num_arbitrations = 0
balances_update_countdown = 0
for iteration in range(num_iterations):

    if simulate:
        logging.info("---------- SIMULATION ----------")
    else:
        logging.info("---------- ARBITRATION ----------")
    logging.info("Time: {}".format(datetime.datetime.now()))

    if balances_update_countdown <= 0:
        logging.info("Retrieving account balances")
        balances_update_countdown = balance_update_interval

        # Get Kraken account balance
        kraken_balance = ccxt_retry(kraken.fetchBalance)
        kraken_balance_fiat = kraken_balance[fiat]["free"]
        kraken_balance_crypto = kraken_balance[crypto]["free"]
        gdax_balance = ccxt_retry(gdax.fetchBalance)
        gdax_balance_fiat = gdax_balance[fiat]["free"]
        gdax_balance_crypto = gdax_balance[crypto]["free"]

        # Get server time to check for recent orders later on
        kraken_server_time = None
        try:
            kraken_server_time = kraken_client.query_public("Time")
            if len(kraken_server_time["error"]) > 0:
                raise Exception("{}".format(kraken_server_time["error"]))
        except Exception as e:
            logging.info("Unable to get server time ({}).".format(e))
            logging.info("Waiting ...")
            logging.info("")
            time.sleep(trial_sleep_time)
            continue
        kraken_server_time = kraken_server_time["result"]["unixtime"]
        logging.info("Kraken server time: {:d}".format(kraken_server_time))
    else:
        balances_update_countdown -= 1

    logging.info("Kraken account balance:")
    logging.info("  {:.2f} {}".format(kraken_balance_fiat, fiat))
    logging.info("  {:.4f} {}".format(kraken_balance_crypto, crypto))
    logging.info("Current Kraken fee: {:.2f} %".format(100 * kraken_fee))
    logging.info("Gdax account balance")
    logging.info("  {:.2f} {}".format(gdax_balance_fiat, fiat))
    logging.info("  {:.4f} {}".format(gdax_balance_crypto, crypto))

    total_balance_fiat = kraken_balance_fiat + gdax_balance_fiat
    total_balance_crypto = kraken_balance_crypto + gdax_balance_crypto
    logging.info("Total balance fiat: {:.2f} {}".format(total_balance_fiat, fiat))
    logging.info("Total balance crypto: {:.4f} {}".format(total_balance_crypto, crypto))

    if total_balance_fiat_begin is None:
        total_balance_fiat_begin = total_balance_fiat

    # Compute sell and buy volumes of crypto currency
    order_volume_crypto = input_order_volume_crypto
    if order_volume_crypto is None:
        order_volume_crypto = max_volume_crypto
    order_volume_crypto = round(order_volume_crypto, crypto_ndigits)

    # Get "safe" price on Kraken for buying and selling ETH
    min_volume = min_volume_factor * order_volume_crypto
    kraken_ob = ccxt_retry(kraken.fetchL2OrderBook, symbol)
    gdax_ob = ccxt_retry(gdax.fetchL2OrderBook, symbol)
    kraken_ask_price, kraken_ask_volume, kraken_bid_price, kraken_bid_volume \
        = ccxt_utils.get_conservative_ask_bid_price(kraken_ob, min_volume)
    gdax_ask_price, gdax_ask_volume, gdax_bid_price, gdax_bid_volume \
        = ccxt_utils.get_conservative_ask_bid_price(gdax_ob, min_volume)

    if kraken_ask_volume < min_volume or kraken_bid_volume < min_volume:
        logging.info("Not enough trading volume on Kraken.")
        logging.info("Kraken ask volume: {}, Kraken bid volume: {}".format(kraken_ask_volume, kraken_bid_volume))
        logging.info("Waiting ...")
        logging.info("")
        time.sleep(trial_sleep_time)
        continue
    if gdax_ask_volume < min_volume or gdax_bid_volume < min_volume:
        logging.info("Not enough trading volume on Gdax.")
        logging.info("Gdax ask volume: {}, Gdax bid volume: {}".format(gdax_ask_volume, gdax_bid_volume))
        logging.info("Waiting ...")
        logging.info("")
        time.sleep(trial_sleep_time)
        continue

    kraken_ask_price = round(kraken_ask_price, fiat_ndigits)
    kraken_bid_price = round(kraken_bid_price, fiat_ndigits)
    assert kraken_ask_price >= kraken_bid_price
    gdax_ask_price = round(gdax_ask_price, fiat_ndigits)
    gdax_bid_price = round(gdax_bid_price, fiat_ndigits)
    assert gdax_ask_price >= gdax_bid_price

    # Remember time so we can cancel if adding Kraken order takes too long.
    order_book_request_time = time.time()

    def compute_arbitrage_gain(ask_price, ask_volume, buy_fee,
            bid_price, bid_volume, sell_fee, order_volume_crypto):
        assert ask_volume >= order_volume_crypto
        assert bid_volume >= order_volume_crypto
        logging.info("Expected buy price: {:.2f} {}".format(ask_price, fiat))
        ask_price_with_fee = ask_price * (1 + buy_fee)
        logging.info("Expected buy price (plus fees): {:.2f} {}".format(ask_price_with_fee, fiat))
        logging.info("Volume for buy price {:.2f} {}: {:.2f}".format(ask_price, fiat, ask_volume))
        buy_volume_fiat = ask_price_with_fee * order_volume_crypto

        logging.info("Expected sell price: {:.2f} {}".format(bid_price, fiat))
        bid_price_with_fee = bid_price * (1 - sell_fee)
        logging.info("Expected sell price (minus fees): {:.2f} {}".format(bid_price_with_fee, fiat))
        logging.info("Volume for sell price {:.2f} {}: {:.2f}".format(bid_price, fiat, bid_volume))
        sell_volume_fiat = bid_price_with_fee * order_volume_crypto

        logging.info("Buy crypto volume (limit price {:.2f} {}: {:.4f} {}".format(
            ask_price, fiat,
            order_volume_crypto, crypto))
        logging.info("Sell crypto volume (limit price {:.2f} {}: {:.4f} {}".format(
            bid_price, fiat,
            order_volume_crypto, crypto))
        logging.info("Expected buy fiat volume (plus fees): {:.2f} {}".format(buy_volume_fiat, fiat))
        logging.info("Expected sell fiat volume (minus fees): {:.2f} {}".format(sell_volume_fiat, fiat))

        gain_fiat = sell_volume_fiat - buy_volume_fiat
        relative_gain = gain_fiat / buy_volume_fiat
        logging.info("Expected gain: {:.2f} {} ({:.4f} %)".format(
            gain_fiat, fiat, 100 * relative_gain))

        return gain_fiat, relative_gain, buy_volume_fiat

    exp_gains_fiat = {}
    exp_relative_gains = {}
    buy_volumes_fiat = {}
    logging.info("----- Buy on Kraken, sell on Gdax -----")
    exp_gains_fiat[BUY_KRAKEN_SELL_GDAX], exp_relative_gains[BUY_KRAKEN_SELL_GDAX], buy_volumes_fiat[BUY_KRAKEN_SELL_GDAX] \
        = compute_arbitrage_gain(
            kraken_ask_price, kraken_ask_volume, kraken_fee,
            gdax_bid_price, gdax_bid_volume, gdax_fee,
            order_volume_crypto)
    logging.info("----- Buy on Gdax, sell on Kraken -----")
    exp_gains_fiat[BUY_GDAX_SELL_KRAKEN], exp_relative_gains[BUY_GDAX_SELL_KRAKEN], buy_volumes_fiat[BUY_GDAX_SELL_KRAKEN] \
        = compute_arbitrage_gain(
            gdax_ask_price, gdax_ask_volume, gdax_fee,
            kraken_bid_price, kraken_bid_volume, kraken_fee,
            order_volume_crypto)

    try:
        with open(os.path.join(home_folder, 'ccxt_arbitration_gains.log'), 'a') as fout:
            iso_time = datetime.datetime.now().isoformat()
            fout.write("{:s} {:f} {:f} {:f} {:f} {:f} {:f} {:f} {:f} {:f} {:f} {:f} {:f}\n".format(
                iso_time, exp_gains_fiat[BUY_KRAKEN_SELL_GDAX], exp_gains_fiat[BUY_GDAX_SELL_KRAKEN],
                exp_relative_gains[BUY_KRAKEN_SELL_GDAX], exp_relative_gains[BUY_GDAX_SELL_KRAKEN],
                kraken_ask_price, kraken_ask_volume, kraken_bid_price, kraken_bid_volume,
                gdax_ask_price, gdax_ask_volume, gdax_bid_price, gdax_bid_volume))
    except:
        pass

    valid = {}
    valid[BUY_GDAX_SELL_KRAKEN] = False
    valid[BUY_KRAKEN_SELL_GDAX] = False
    chosen_min_relative_gains = {}

    for min_relative_gain, min_fiat_reserve in zip(min_relative_gains[BUY_GDAX_SELL_KRAKEN], min_fiat_reserves[BUY_GDAX_SELL_KRAKEN]):
        if exp_relative_gains[BUY_GDAX_SELL_KRAKEN] >= min_relative_gain \
        and gdax_balance_fiat / total_balance_fiat >= min_fiat_reserve \
        and gdax_balance_fiat >= buy_safety_factor_fiat * order_volume_crypto * buy_volumes_fiat[BUY_GDAX_SELL_KRAKEN] \
        and kraken_balance_crypto >= order_volume_crypto:
            logging.info("{} is possible with min_relative_gain={} %, min_fiat_reserve={}".
                format(ARBITRATION_MODES_STR[BUY_GDAX_SELL_KRAKEN], 100 * min_relative_gain, min_fiat_reserve))
            valid[BUY_GDAX_SELL_KRAKEN] = True
            chosen_min_relative_gains[BUY_GDAX_SELL_KRAKEN] = min_relative_gain
            break

    for min_relative_gain, min_fiat_reserve in zip(min_relative_gains[BUY_KRAKEN_SELL_GDAX], min_fiat_reserves[BUY_KRAKEN_SELL_GDAX]):
        if exp_relative_gains[BUY_KRAKEN_SELL_GDAX] >= min_relative_gain \
        and kraken_balance_fiat / total_balance_fiat >= min_fiat_reserve \
        and kraken_balance_fiat >= buy_safety_factor_fiat * order_volume_crypto * buy_volumes_fiat[BUY_KRAKEN_SELL_GDAX] \
        and gdax_balance_crypto >= order_volume_crypto:
            logging.info("{} is possible with min_relative_gain={} %, min_fiat_reserve={}".
                format(ARBITRATION_MODES_STR[BUY_KRAKEN_SELL_GDAX], 100 * min_relative_gain, min_fiat_reserve))
            valid[BUY_KRAKEN_SELL_GDAX] = True
            chosen_min_relative_gains[BUY_KRAKEN_SELL_GDAX] = min_relative_gain
            break

    valid_values = [value for key, value in valid.items()]
    if not np.any(valid_values):
        logging.info("No arbitration opportunity. Cancelling.")
        logging.info("Waiting ...")
        logging.info("")
        time.sleep(trial_sleep_time)
        continue

    for arbitration_mode in ARBITRATION_MODES:
        if not valid[arbitration_mode]:
            exp_relative_gains[arbitration_mode] = -float("inf")

    if exp_relative_gains[BUY_GDAX_SELL_KRAKEN] > exp_relative_gains[BUY_KRAKEN_SELL_GDAX]:
        arbitration_mode = BUY_GDAX_SELL_KRAKEN
    else:
        arbitration_mode = BUY_KRAKEN_SELL_GDAX

    # if exp_relative_gains[arbitration_mode] < chosen_min_relative_gains[arbitration_mode]:
    #     logging.info("Gain is too low. Cancelling.")
    #     logging.info("Waiting ...")
    #     logging.info("")
    #     time.sleep(trial_sleep_time)
    #     continue

    if arbitration_mode == BUY_KRAKEN_SELL_GDAX:
        if order_volume_crypto > gdax_balance_crypto:
            logging.info("Not enough crypto balance in Gdax account. Reducing order amount.")
            order_volume_crypto = gdax_balance_crypto
        if buy_volumes_fiat[arbitration_mode] * buy_safety_factor_fiat > kraken_balance_fiat:
            logging.info("Not enough fiat balance in Kraken account. Reducing order amount.")
            reduce_factor = kraken_balance_fiat / (buy_volumes_fiat[arbitration_mode] * buy_safety_factor_fiat)
            order_volume_crypto = round(reduce_factor * order_volume_crypto, crypto_ndigits)
            logging.info("Reduced order amount to {:.4f} {}".format(order_volume_crypto, crypto))
    elif arbitration_mode == BUY_GDAX_SELL_KRAKEN:
        if order_volume_crypto > kraken_balance_crypto:
            logging.info("Not enough crypto balance in Kraken account. Reducing order amount.")
            order_volume_crypto = kraken_balance_crypto
        if buy_volumes_fiat[arbitration_mode] * buy_safety_factor_fiat > gdax_balance_fiat:
            logging.info("Not enough fiat balance in Gdax account. Reducing order amount.")
            reduce_factor = gdax_balance_fiat / (buy_volumes_fiat[arbitration_mode] * buy_safety_factor_fiat)
            order_volume_crypto = round(reduce_factor * order_volume_crypto, crypto_ndigits)
            logging.info("Reduced order amount to {:.4f} {}".format(order_volume_crypto, crypto))

    logging.info("Gain is high enough. Continuing.")
    if arbitration_mode == BUY_KRAKEN_SELL_GDAX:
        logging.info("Arbitration mode: BUY_KRAKEN_SELL_GDAX")
    else:
        logging.info("Arbitration mode: BUY_GDAX_SELL_KRAKEN")

    if prompt_user and not prompt_yes_no("Continue?"):
        logging.info("Cancelling")
        sys.exit(1)

    if simulate:
        logging.info("Simulated arbitration done.")
        logging.info("")
        time.sleep(10)
        continue

    # Make sure we check balances on next iteration in case something goes wrong.
    balances_update_countdown = 0

    #
    # Kraken Add buy order
    #

    def find_matching_orders(orders, match_lambda):
        matching_order_ids = []
        for order in orders.items():
            if match_lamdba(order):
                matching_order_ids.append(order["id"])
        return matching_order_ids

    kraken_userref = int(random.randint(0, 2**31 - 1))
    gdax_userref = str(uuid.uuid4())
    max_order_time = order_book_request_time + max_time_from_order_book_to_order

    def kraken_find_matching_orders(order_dict, userref):
        matching_order_ids = []
        for order_id, order in order_dict.items():
            if order["userref"] == userref:
                matching_order_ids.append(order_id)
        return matching_order_ids

    # TODO
    def kraken_get_open_orders():
        data = {}
        response = kraken_client.query_private('OpenOrders', data=data, timeout=kraken_timeout)
        if "result" in response:
            response["result"] = response["result"]["open"]
        return response

    # TODO
    def kraken_get_closed_orders():
        data = {
            "start": kraken_server_time,
        }
        response = kraken_client.query_private('ClosedOrders', data=data, timeout=kraken_timeout)
        if "result" in response:
            response["result"] = response["result"]["closed"]
        return response

    def check_order_info(check_order_time, match_lamdba):
        # logging.info("kraken_open_orders:", open_orders)
        matching_order_ids = []
        check_order_start_time = time.time()
        while len(matching_order_ids) == 0:
            logging.info("Trying to find order (time={}, kraken_time={}) ...".format(
                datetime.datetime.now(), kraken_server_time))
            check_order_time_limit_reached = time.time() - check_order_start_time > check_order_time
            open_orders = kraken_wrapper.retry_on_error(
                kraken_get_open_orders)
            logging.info("Open orders: {}".format(open_orders))
            matching_order_ids = kraken_find_matching_orders(open_orders, kraken_userref)
            if len(matching_order_ids) == 0:
                closed_orders = kraken_wrapper.retry_on_error(
                    kraken_get_closed_orders)
                logging.info("Closed orders: {}".format(closed_orders))
                # logging.info("kraken_closed_orders:", closed_orders)
                matching_order_ids = kraken_find_matching_orders(closed_orders, kraken_userref)
            if len(matching_order_ids) == 0 and check_order_time_limit_reached:
                return None
            if len(matching_order_ids) > 1:
                logging.warning("WARNING: Multiple matching orders found.")
                logging.warning("Matching orders: {}".format(matching_order_ids))
        order_id = matching_order_ids[0]
        return order_id

    def kraken_check_order_info(check_order_time, userref):
        return check_order_info(check_order_time, lambda order: order["info"]["userref"] == userref)

    def gdax_check_order_info(check_order_time, userref):
        return check_order_info(check_order_time, lambda order: order["info"]["client_oid"] == userref)

    if arbitration_mode == BUY_KRAKEN_SELL_GDAX:
        # buy_volume_fiat = kraken_buy_price_fiat * order_volume_crypto * (1 + kraken_fee_ratio)
        # if buy_volume_fiat * buy_safety_factor_fiat > kraken_balance_fiat:
        #     logging.info("Not enough fiat balance in Kraken account for transfer.")
        #     logging.info("Cancelling.")
        #     logging.info(trial_sleep_time)
        #     continue

        # Limit total losses if market moves extremely fast (if the market recovers again)
        buy_limit_price_fiat = limit_price_safety_factor * kraken_ask_price
        # if kraken_ask_price >= gdax_bid_price:
        #     buy_limit_price_fiat = kraken_ask_price + kraken_ask_price - gdax_bid_price
        # else:
        #     buy_limit_price_fiat = (kraken_ask_price + gdax_bid_price) / 2.0
        buy_limit_price_fiat = round(buy_limit_price_fiat, fiat_ndigits)
        logging.info("Creating Kraken buy order for {:.4f} {} (limit price {:f}) (userref={:d})".format(
            order_volume_crypto, crypto, buy_limit_price_fiat, kraken_userref))
        kraken_order_result = ccxt_retry(kraken.createLimitBuyOrder,
            symbol, order_volume_crypto, buy_limit_price_fiat, {"userref": kraken_userref},
            _max_time=max_order_time, _max_trials=1)
        if kraken_order_result is not None:
            kraken_order_id = kraken_order_result["id"]
        else:
            logging.warning("Order submission failed.")
            kraken_order_id = kraken_check_order_info(check_order_time, kraken_userref)
            if kraken_order_id is None:
                logging.info("Kraken order did not go through.")
                logging.info("Trying another iteration.")
                logging.info("")
                continue
        logging.info("Kraken order id: {}".format(kraken_order_id))

        #
        # GDax Sell
        #

        # Limit total losses if market moves extremely fast (if the market recovers again)
        sell_limit_price_fiat = gdax_bid_price / limit_price_safety_factor
        # if kraken_ask_price >= gdax_bid_price:
        #     sell_limit_price_fiat = kraken_ask_price - (kraken_ask_price - gdax_bid_price)
        # else:
        #     sell_limit_price_fiat = (kraken_ask_price + gdax_bid_price) / 2.0
        sell_limit_price_fiat = round(sell_limit_price_fiat, fiat_ndigits)
        logging.info("Creating Gdax sell order for {:.4f} {} (limit price {:f}) (userref={})".format(
            order_volume_crypto, crypto, sell_limit_price_fiat, gdax_userref))
        max_order_time = time.time() + max_time_from_order_book_to_order
        gdax_order_result = ccxt_retry(gdax.createLimitSellOrder,
            symbol, order_volume_crypto, sell_limit_price_fiat, {"client_oid": gdax_userref},
            _max_time=max_order_time, _max_trials=1)
        if gdax_order_result is not None:
            gdax_order_id = gdax_order_result["id"]
        else:
            logging.warning("Exceeded time limit between order book request and order.")
            gdax_order_id = gdax_check_order_info(check_order_time, gdax_userref)
            if gdax_order_id is None:
                logging.error("ERROR: Gdax order did not go through. Stopping.")
                sys.exit(1)
        # TODO: Check for errors message {'message': 'size too precise (7.020050523748998)'}
        logging.info("Gdax order id: {}".format(gdax_order_id))

    elif arbitration_mode == BUY_GDAX_SELL_KRAKEN:
        # buy_volume_fiat = gdax_buy_price_fiat * order_volume_crypto * (1 + gdax_fee_ratio)
        # if buy_volume_fiat * buy_safety_factor_fiat > gdax_balance_fiat:
        #     logging.info("Not enough fiat balance in Gdax account.")
        #     logging.info("Cancelling.")
        #     logging.info(trial_sleep_time)
        #     continue

        # Limit total losses if market moves extremely fast (if the market recovers again)
        sell_limit_price_fiat = kraken_bid_price / limit_price_safety_factor
        # if gdax_ask_price >= kraken_bid_price:
        #     sell_limit_price_fiat = gdax_ask_price - (gdax_ask_price - kraken_bid_price)
        # else:
        #     sell_limit_price_fiat = (gdax_ask_price + kraken_bid_price) / 2.0
        sell_limit_price_fiat = round(sell_limit_price_fiat, fiat_ndigits)
        logging.info("Creating Kraken sell order for {:.4f} {} (limit price {:f}) (userref={:d})".format(
            order_volume_crypto, crypto, sell_limit_price_fiat, kraken_userref))
        kraken_order_result = ccxt_retry(kraken.createLimitSellOrder,
            symbol, order_volume_crypto, sell_limit_price_fiat, {"userref": kraken_userref},
            _max_time=max_order_time, _max_trials=1)
        if kraken_order_result is not None:
            kraken_order_id = kraken_order_result["id"]
        else:
            logging.warning("Exceeded time limit between order book request and order.")
            kraken_order_id = kraken_check_order_info(check_order_time, kraken_userref)
            if kraken_order_id is None:
                logging.info("Kraken order did not go through.")
                logging.info("Trying another iteration.")
                logging.info("")
                continue
        logging.info("Kraken order id: {}".format(kraken_order_id))

        #
        # GDax Sell
        #

        # Limit total losses if market moves extremely fast (if the market recovers again)
        buy_limit_price_fiat = gdax_ask_price * limit_price_safety_factor
        # if gdax_ask_price >= kraken_bid_price:
        #     buy_limit_price_fiat = gdax_ask_price + gdax_ask_price - kraken_bid_price
        # else:
        #     buy_limit_price_fiat = (gdax_ask_price + kraken_bid_price) / 2.0
        buy_limit_price_fiat = round(buy_limit_price_fiat, fiat_ndigits)
        logging.info("Creating Gdax buy order for {:.4f} {} (limit price {:f}) (userref={})".format(
            order_volume_crypto, crypto, buy_limit_price_fiat, gdax_userref))
        max_order_time = time.time() + max_time_from_order_book_to_order
        gdax_order_result = ccxt_retry(gdax.createLimitBuyOrder,
            symbol, order_volume_crypto, buy_limit_price_fiat, {"client_oid": gdax_userref},
            _max_time=max_order_time, _max_trials=1)
        if gdax_order_result is not None:
            gdax_order_id = gdax_order_result["id"]
        else:
            logging.warning("Exceeded time limit between order book request and order.")
            gdax_order_id = gdax_check_order_info(check_order_time, gdax_userref)
            if gdax_order_id is None:
                logging.error("ERROR: Gdax order did not go through. Stopping.")
                sys.exit(1)
        # TODO: Check for errors message {'message': 'size too precise (7.020050523748998)'}
        logging.info("Gdax order id: {}".format(gdax_order_id))

    else:
        raise RuntimeError("Unknown arbitration mode: {}".format(arbitration_mode))

    #
    # Wait for orders to finish
    #

    def is_order_done(order_info):
        # logging.info("Order status: {}".format(order_info["status"]))
        if order_info["status"] == "closed":
            return True
        elif order_info["status"] == "canceled":
            logging.error("Order was cancelled.")
            logging.error("Exiting")
            sys.exit(1)
        elif order_info["status"] == "expired":
            logging.error("Order expired.")
            logging.error("Exiting")
            sys.exit(1)
        return False

    gdax_order_done = False
    kraken_order_done = False
    logging.info("Waiting for orders to finish...")
    while (not gdax_order_done) or (not kraken_order_done):
        if not kraken_order_done:
            logging.info("Checking Kraken order...")
            kraken_order_info = ccxt_retry(kraken.fetchOrder, kraken_order_id)
            assert kraken_order_info is not None
            if is_order_done(kraken_order_info):
                kraken_order_done = True
                # logging.info("Kraken order info:", kraken_order_info)
                if arbitration_mode == BUY_KRAKEN_SELL_GDAX:
                    logging.info("Final buy price: {} {}".format(kraken_order_info["cost"] / kraken_order_info["filled"], fiat))
                    if kraken_order_info["fee"] is not None:
                        logging.info("Final buy fee: {} {}".format(kraken_order_info["fee"]["cost"], kraken_order_info["fee"]["currency"]))
                    else:
                        logging.info("No fee information")
                else:
                    logging.info("Final sell price: {} {}".format(kraken_order_info["cost"] / kraken_order_info["filled"], fiat))
                    if kraken_order_info["fee"] is not None:
                        logging.info("Final sell fee: {} {}".format(kraken_order_info["fee"]["cost"], kraken_order_info["fee"]["currency"]))
                    else:
                        logging.info("No fee information")
        if not gdax_order_done:
            logging.info("Checking Gdax order...")
            gdax_order_info = ccxt_retry(gdax.fetchOrder, gdax_order_id)
            assert gdax_order_info is not None
            if is_order_done(gdax_order_info):
                gdax_order_done = True
                # logging.info("Gdax order info:", gdax_order_info)
                if arbitration_mode == BUY_KRAKEN_SELL_GDAX:
                    logging.info("Final sell price: {} {}".format(gdax_order_info["cost"] / gdax_order_info["filled"], fiat))
                    if "fill_fees" in gdax_order_info["info"]:
                        logging.info("Final sell fee: {} {}".format(float(gdax_order_info["info"]["fill_fees"]), fiat))
                    else:
                        logging.info("No fee information")
                else:
                    logging.info("Final buy price: {} {}".format(gdax_order_info["cost"] / gdax_order_info["filled"], fiat))
                    if "fill_fees" in gdax_order_info["info"]:
                        logging.info("Final buy fee: {} {}".format(float(gdax_order_info["info"]["fill_fees"]), fiat))
                    else:
                        logging.info("No fee information")
        if (not gdax_order_done) or (not kraken_order_done):
            # Wait a bit before doing another check.
            time.sleep(order_check_interval)
    logging.info("Orders finished.")

    num_arbitrations += 1

    kraken_balance_after = ccxt_retry(kraken.fetchBalance)
    kraken_balance_fiat_after = kraken_balance_after[fiat]["free"]
    kraken_balance_crypto_after = kraken_balance_after[crypto]["free"]
    gdax_balance_after = ccxt_retry(gdax.fetchBalance)
    gdax_balance_fiat_after = gdax_balance_after[fiat]["free"]
    gdax_balance_crypto_after = gdax_balance_after[crypto]["free"]

    logging.info("Gdax account balance before arbitration")
    logging.info("  {:.2f} {}".format(gdax_balance_fiat, fiat))
    logging.info("  {:.4f} {}".format(gdax_balance_crypto, crypto))
    logging.info("Gdax account balance after arbitration")
    logging.info("  {:.2f} {}".format(gdax_balance_fiat_after, fiat))
    logging.info("  {:.4f} {}".format(gdax_balance_crypto_after, crypto))

    logging.info("Kraken account balance before arbitration:")
    logging.info("  {:.2f} {}".format(kraken_balance_fiat, fiat))
    logging.info("  {:.4f} {}".format(kraken_balance_crypto, crypto))
    logging.info("Kraken account balance after arbitration:")
    logging.info("  {:.2f} {}".format(kraken_balance_fiat_after, fiat))
    logging.info("  {:.4f} {}".format(kraken_balance_crypto_after, crypto))

    total_balance_fiat_before = gdax_balance_fiat + kraken_balance_fiat
    total_balance_fiat_after = gdax_balance_fiat_after + kraken_balance_fiat_after
    total_balance_crypto_before = gdax_balance_crypto + kraken_balance_crypto
    total_balance_crypto_after = gdax_balance_crypto_after + kraken_balance_crypto_after
    gain_fiat = total_balance_fiat_after - total_balance_fiat_before
    gain_crypto = total_balance_crypto_after - total_balance_crypto_before
    if arbitration_mode == BUY_KRAKEN_SELL_GDAX:
        invested_fiat = kraken_balance_fiat - kraken_balance_fiat_after
    else:
        invested_fiat = gdax_balance_fiat - gdax_balance_fiat_after
    relative_gain = gain_fiat / invested_fiat

    logging.info("Total balance fiat: {:.2f} {}".format(total_balance_fiat_after, fiat))
    logging.info("Total balance crypto: {:.4f} {}".format(total_balance_crypto_after, crypto))
    logging.info("Gain in fiat: {:.2f} {} ({:.4f} %)".format(
        gain_fiat, fiat, 100 * relative_gain))
    logging.info("Gain in crypto: {:.4f} {}".format(gain_crypto, crypto))

    if relative_gain < exp_relative_gains[arbitration_mode]:
        logging.warning("WARNING: Actual gain was less than expected gain.")

    # if ( relative_gain < 0 and exp_relative_gains[arbitration_mode] > 0 ) \
    # or ( relative_gain < exp_relative_gains[arbitration_mode] ):
    #     logging.error("ERROR: Lost {:.2f} {}.".format(-gain_fiat, fiat))
    #     logging.error("Exiting")
    #     sys.exit(1)
    if ( relative_gain < 0 and relative_gain < (chosen_min_relative_gains[arbitration_mode] / safety_lower_gain_tolerance) ) \
    or ( relative_gain >= 0 and relative_gain < (chosen_min_relative_gains[arbitration_mode] * safety_lower_gain_tolerance) ):
        logging.warning("ERROR: Actual gain was far less than desired minimum gain.")
        # logging.warning("Exiting")
        # sys.exit(1)
    elif relative_gain < chosen_min_relative_gains[arbitration_mode]:
        logging.warning("WARNING: Actual gain was less than desired minimum gain.")

    if abs(gain_crypto) > max_balance_deviation_crypto:
        logging.error("ERROR: Difference in total crypto balance is too high.")
        logging.error("Exiting")
        sys.exit(1)

    logging.info("Arbitration done.")
    logging.info("Number of arbitrations done: {:d}".format(num_arbitrations))

    gain_fiat_since_begin = total_balance_fiat_after - total_balance_fiat_begin
    logging.info("Total gain since start: {:.2f} {}".format(gain_fiat_since_begin, fiat))
    logging.info("")

    if gain_fiat_since_begin < - max_overall_fiat_loss:
        logging.error("ERROR: Overall fiat loss is too high.")
        logging.error("Exiting")
        sys.exit(1)

    if num_arbitrations >= max_num_arbitrations:
        logging.info("Stopping")
        logging.info("")
        break
