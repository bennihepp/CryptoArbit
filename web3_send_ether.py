import sys
import json
import getpass
import ethereum
import ethereum.tools
import eth_utils
import web3

def get_private_key(keystore):
    password = getpass.getpass("Passphrase to unlock keystore: ", stream=None)
    private_key_bytes = ethereum.tools.keys.decode_keystore_json(keystore, password)
    private_key = ethereum.utils.encode_hex(private_key_bytes)
    return private_key

def prompt_yes_no(message):
    response = input("{} [yes,no] ".format(message))
    if response == "yes":
        return True
    return False

web3_endpoint = "https://api.myetherapi.com/eth"
gas_price_gwei = 25
gas = 21000

keystore_file = sys.argv[1]
to_address = sys.argv[2]
value_eth = float(sys.argv[3])

keystore = json.load(open(keystore_file, "r"))
address = "0x{}".format(keystore["address"])
address = eth_utils.to_checksum_address(address)
print("Loaded keystore for address {}".format(address))

w3 = web3.Web3(web3.HTTPProvider(web3_endpoint))

value_wei = w3.toWei(value_eth, "ether")
gas_price_wei = w3.toWei(gas_price_gwei, "gwei")
nonce = w3.eth.getTransactionCount(address)
#chain_id = w3.eth.net.getId()
chain_id = 1
transaction = {
    'to': to_address,
    'value': value_wei,
    'gas': gas,
    'gasPrice': gas_price_wei,
    'nonce': nonce,
    'chainId': chain_id,
    }
#print("Transaction: {}".format(transaction))

private_key = get_private_key(keystore)
signed = w3.eth.account.signTransaction(transaction, private_key)
#print("Signed transaction: {}".format(signed))
#print("Raw signed transaction: {}".format(signed.rawTransaction))

print("Transaction to send {} ETH to {}".format(value_eth, to_address))
print("Transaction cost: {}".format(w3.fromWei(gas * gas_price_wei, "ether")))

if prompt_yes_no("Send transaction"):
    print("Sending transaction")
    reply_raw = w3.eth.sendRawTransaction(signed.rawTransaction)
    print("Done.")
    #print("Raw TX hash: {}".format(reply_raw))
    reply = "0x{}".format(ethereum.utils.encode_hex(reply_raw))
    print("TX hash: {}".format(reply))

