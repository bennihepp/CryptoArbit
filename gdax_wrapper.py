import datetime
import dateutil.parser


def read_keys_from_file(filename):
    with open(filename, "r") as fin:
        passphrase = fin.readline().strip()
        api_key = fin.readline().strip()
        api_secret = fin.readline().strip()
    return api_key, api_secret, passphrase


def create_client_from_file(filename):
	with open(filename, "r") as fin:
		passphrase = fin.readline().strip()
		api_key = fin.readline().strip()
		api_secret = fin.readline().strip()
	return create_client(api_key, api_secret, passphrase)


def create_client(api_key, api_secret, passphrase):
	import gdax
	client = gdax.AuthenticatedClient(api_key, api_secret, passphrase)
	return client


def get_account_balance_and_currency(client, account_id):
	account = client.get_account(account_id)
	return float(account["balance"]), account["currency"]


def get_account_balance(client, account_id):
	balance, _ = get_account_balance_and_currency(client, account_id)
	return balance

def get_ohlc(client, product_id, granularity, num_slices):
	resp = client.get_time()
	t_end = dateutil.parser.parse(resp["iso"])
	t_start = t_end - datetime.timedelta(seconds=num_slices * granularity)
	response = client.get_product_historic_rates(product_id=product_id,
		start=t_start.isoformat(),
		end=t_end.isoformat(),
		granularity=granularity)
	return response
